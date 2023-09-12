/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.classic.manager;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.config.BucketType;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpClient;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.endpoint.http.CoreHttpResponse;
import com.couchbase.client.core.error.BucketExistsException;
import com.couchbase.client.core.error.BucketNotFlushableException;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.manager.CoreBucketManagerOps;
import com.couchbase.client.core.manager.bucket.CoreBucketSettings;
import com.couchbase.client.core.manager.bucket.CoreCreateBucketSettings;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.util.UrlQueryStringBuilder;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.couchbase.client.core.config.BucketType.MEMCACHED;
import static com.couchbase.client.core.endpoint.http.CoreHttpPath.path;
import static com.couchbase.client.core.error.HttpStatusCodeException.couchbaseResponseStatus;
import static com.couchbase.client.core.error.HttpStatusCodeException.httpResponseBody;
import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbThrowables.propagate;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class ClassicCoreBucketManager implements CoreBucketManagerOps {
  private final Core core;
  private final CoreHttpClient httpClient;

  public ClassicCoreBucketManager(final Core core) {
    this.core = core;
    this.httpClient = core.httpClient(RequestTarget.manager());
  }

  private static CoreHttpPath pathForBuckets() {
    return path("/pools/default/buckets/");
  }

  private static CoreHttpPath pathForBucket(final String bucketName) {
    return path("/pools/default/buckets/{bucketName}",
        mapOf("bucketName", bucketName));
  }

  private static CoreHttpPath pathForBucketFlush(final String bucketName) {
    return path("/pools/default/buckets/{bucketName}/controller/doFlush",
        mapOf("bucketName", bucketName));
  }

  public CompletableFuture<Void> createBucket(CoreBucketSettings settings, CoreCreateBucketSettings createSpecificSettings, CoreCommonOptions options) {

    String bucketName = settings.name();
    return httpClient.post(pathForBuckets(), options)
        .trace(TracingIdentifiers.SPAN_REQUEST_MB_CREATE_BUCKET)
        .traceBucket(bucketName)
        .form(convertSettingsToParams(settings, createSpecificSettings, false))
        .exec(core)
        .exceptionally(t -> {
          throw httpResponseBody(t).contains("Bucket with given name already exists")
              ? BucketExistsException.forBucket(bucketName)
              : propagate(wrap(t));
        })
        .thenApply(response -> null);
  }

  private static Throwable wrap(Throwable t) {
    if (t instanceof CompletionException) {
      return wrap(t.getCause());
    }
    if (t instanceof HttpStatusCodeException) {
      HttpStatusCodeException err = (HttpStatusCodeException) t;
      if (err.httpStatusCode() == 400) {
        return new InvalidArgumentException(t.getMessage(), t, err.context());
      }
    }
    return t;
  }

  public CompletableFuture<Void> updateBucket(CoreBucketSettings settings, final CoreCommonOptions options) {
    String bucketName = settings.name();

    RequestSpan span = CbTracing.newSpan(core.context(), TracingIdentifiers.SPAN_REQUEST_MB_UPDATE_BUCKET, options.parentSpan().orElse(null));
    span.attribute(TracingIdentifiers.ATTR_NAME, bucketName);
    CoreCommonOptions getAllBucketOptions = options.withParentSpan(span);

    return Mono
        .fromFuture(() -> getAllBuckets(getAllBucketOptions))
        .map(buckets -> buckets.containsKey(bucketName))
        .flatMap(bucketExists -> {
          if (!bucketExists) {
            return Mono.error(BucketNotFoundException.forBucket(bucketName));
          }
          return Mono.fromFuture(
              httpClient.post(pathForBucket(bucketName), options)
                  .form(convertSettingsToParams(settings, null, true))
                  .exec(core)
                  .exceptionally(t -> {
                    throw propagate(wrap(t));
                  })
                  .thenApply(response -> null));
        })
        .then()
        .doOnTerminate(span::end)
        .toFuture();
  }

  public CompletableFuture<Void> dropBucket(String bucketName, CoreCommonOptions options) {
    return httpClient.delete(pathForBucket(bucketName), options)
        .trace(TracingIdentifiers.SPAN_REQUEST_MB_DROP_BUCKET)
        .traceBucket(bucketName)
        .exec(core)
        .exceptionally(translateBucketNotFound(bucketName))
        .thenApply(response -> null);
  }

  public CompletableFuture<CoreBucketSettings> getBucket(String bucketName, CoreCommonOptions options) {
    return httpClient.get(pathForBucket(bucketName), options)
        .trace(TracingIdentifiers.SPAN_REQUEST_MB_GET_BUCKET)
        .traceBucket(bucketName)
        .exec(core)
        .exceptionally(translateBucketNotFound(bucketName))
        .thenApply(v -> CoreBucketSettingsJson.create(v.content()));
  }

  private static Function<Throwable, CoreHttpResponse> translateBucketNotFound(String bucketName) {
    return t -> {
      throw couchbaseResponseStatus(t) == ResponseStatus.NOT_FOUND
          ? BucketNotFoundException.forBucket(bucketName)
          : propagate(wrap(t));
    };
  }

  public CompletableFuture<Map<String, CoreBucketSettings>> getAllBuckets(CoreCommonOptions options) {
    return httpClient.get(pathForBuckets(), options)
        .trace(TracingIdentifiers.SPAN_REQUEST_MB_GET_ALL_BUCKETS)
        .exec(core)
        .thenApply(response -> {
          JsonNode tree = Mapper.decodeIntoTree(response.content());
          Map<String, CoreBucketSettings> out = new HashMap<>();
          for (final JsonNode bucket : tree) {
            String bucketName = requireNonNull(bucket.get("name").textValue(), "Bucket json is missing 'name' field: " + redactMeta(bucket));
            out.put(bucketName, CoreBucketSettingsJson.create(bucket));
          }
          return out;
        });
  }

  public CompletableFuture<Void> flushBucket(String bucketName, CoreCommonOptions options) {
    return httpClient.post(pathForBucketFlush(bucketName), options)
        .trace(TracingIdentifiers.SPAN_REQUEST_MB_FLUSH_BUCKET)
        .traceBucket(bucketName)
        .exec(core)
        .exceptionally(t -> {
          if (couchbaseResponseStatus(t) == ResponseStatus.INVALID_ARGS &&
              httpResponseBody(t).contains("Flush is disabled")) {
            throw BucketNotFlushableException.forBucket(bucketName);
          }
          return translateBucketNotFound(bucketName).apply(t);
        })
        .thenApply(response -> null);
  }

  private UrlQueryStringBuilder convertSettingsToParams(CoreBucketSettings settings,
                                                        @Nullable CoreCreateBucketSettings createSpecificSettings,
                                                        boolean update) {
    Map<String, String> params = new HashMap<>();

    params.put("ramQuotaMB", String.valueOf(settings.ramQuotaMB()));
    if (settings.bucketType() != MEMCACHED && settings.numReplicas() != null) {
      params.put("replicaNumber", String.valueOf(settings.numReplicas()));
    }
    if (settings.flushEnabled() != null) {
      params.put("flushEnabled", String.valueOf(settings.flushEnabled() ? 1 : 0));
    }
    if (settings.maxExpiry() != null) {
      long maxTTL = settings.maxExpiry().getSeconds();
      // Do not send if it's been left at default, else will get an error on CE
      if (maxTTL != 0) {
        params.put("maxTTL", String.valueOf(maxTTL));
      }
    }
    if (settings.evictionPolicy() != null) {
      // let server assign the default policy for this bucket type
      params.put("evictionPolicy", settings.evictionPolicy().alias());
    }
    // Do not send if it's been left at default, else will get an error on CE
    if (settings.compressionMode() != null) {
      params.put("compressionMode", settings.compressionMode().alias());
    }

    if (settings.minimumDurabilityLevel() != null && settings.minimumDurabilityLevel() != DurabilityLevel.NONE) {
      params.put("durabilityMinLevel", settings.minimumDurabilityLevel().encodeForManagementApi());
    }
    if (settings.storageBackend() != null) {
      params.put("storageBackend", settings.storageBackend().alias());
    }

    params.put("name", settings.name());
    if (settings.bucketType() != null) {
      params.put("bucketType", settings.bucketType().getRaw());
    }
    if (createSpecificSettings != null && createSpecificSettings.conflictResolutionType() != null) {
      params.put("conflictResolutionType", createSpecificSettings.conflictResolutionType().alias());
    }
    if (settings.bucketType() != null
        && settings.bucketType() != BucketType.EPHEMERAL
        && settings.replicaIndexes() != null) {
      params.put("replicaIndex", String.valueOf(settings.replicaIndexes() ? 1 : 0));
    }

    if (settings.historyRetentionCollectionDefault() != null) {
      params.put("historyRetentionCollectionDefault", settings.historyRetentionCollectionDefault().toString());
    }

    if (settings.historyRetentionBytes() != null) {
      params.put("historyRetentionBytes", settings.historyRetentionBytes().toString());
    }

    if (settings.historyRetentionDuration() != null) {
      params.put("historyRetentionSeconds", String.valueOf(TimeUnit.MILLISECONDS.toSeconds(settings.historyRetentionDuration().toMillis())));
    }

    // The following values must not be changed on update
    if (update) {
      params.remove("name");
      params.remove("bucketType");
      params.remove("conflictResolutionType");
      params.remove("replicaIndex");
      params.remove("storageBackend");
    }

    UrlQueryStringBuilder form = UrlQueryStringBuilder.createForUrlSafeNames();
    params.forEach(form::set);
    return form;
  }
}
