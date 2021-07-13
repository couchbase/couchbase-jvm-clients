/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.client.core.manager;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpClient;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.endpoint.http.CoreHttpResponse;
import com.couchbase.client.core.error.BucketExistsException;
import com.couchbase.client.core.error.BucketNotFlushableException;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.util.UrlQueryStringBuilder;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.couchbase.client.core.endpoint.http.CoreHttpPath.path;
import static com.couchbase.client.core.error.HttpStatusCodeException.couchbaseResponseStatus;
import static com.couchbase.client.core.error.HttpStatusCodeException.httpResponseBody;
import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbThrowables.propagate;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreBucketManager {
  private final Core core;
  private final CoreHttpClient httpClient;

  public CoreBucketManager(final Core core) {
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

  private static String getBucketName(Map<String, String> settings) {
    return requireNonNull(settings.get("name"), "Missing required 'name' parameter");
  }

  public CompletableFuture<Void> createBucket(Map<String, String> settings, CoreCommonOptions options) {
    String bucketName = getBucketName(settings);
    return httpClient.post(pathForBuckets(), options)
        .trace(TracingIdentifiers.SPAN_REQUEST_MB_CREATE_BUCKET)
        .traceBucket(bucketName)
        .form(convertSettingsToParams(settings, false))
        .exec(core)
        .exceptionally(t -> {
          throw httpResponseBody(t).contains("Bucket with given name already exists")
              ? BucketExistsException.forBucket(bucketName)
              : propagate(t);
        })
        .thenApply(response -> null);
  }

  public CompletableFuture<Void> updateBucket(Map<String, String> settings, final CoreCommonOptions options) {
    String bucketName = getBucketName(settings);

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
                  .form(convertSettingsToParams(settings, true))
                  .exec(core)
                  .thenApply(response -> null));
        })
        .then()
        .doOnTerminate(span::end)
        .toFuture();
  }

  private UrlQueryStringBuilder convertSettingsToParams(Map<String,String> settings, boolean update) {
    Map<String, String> params = new HashMap<>(settings);

    // The following values must not be changed on update
    if (update) {
      params.remove("name");
      params.remove("bucketType");
      params.remove("conflictResolutionType");
      params.remove("replicaIndex");
    }

    UrlQueryStringBuilder form = UrlQueryStringBuilder.createForUrlSafeNames();
    params.forEach(form::set);
    return form;
  }

  public CompletableFuture<Void> dropBucket(String bucketName, CoreCommonOptions options) {
    return httpClient.delete(pathForBucket(bucketName), options)
        .trace(TracingIdentifiers.SPAN_REQUEST_MB_DROP_BUCKET)
        .traceBucket(bucketName)
        .exec(core)
        .exceptionally(translateBucketNotFound(bucketName))
        .thenApply(response -> null);
  }

  public CompletableFuture<byte[]> getBucket(String bucketName, CoreCommonOptions options) {
    return httpClient.get(pathForBucket(bucketName), options)
        .trace(TracingIdentifiers.SPAN_REQUEST_MB_GET_BUCKET)
        .traceBucket(bucketName)
        .exec(core)
        .exceptionally(translateBucketNotFound(bucketName))
        .thenApply(CoreHttpResponse::content);
  }

  private static Function<Throwable, CoreHttpResponse> translateBucketNotFound(String bucketName) {
    return t -> {
      throw couchbaseResponseStatus(t) == ResponseStatus.NOT_FOUND
          ? BucketNotFoundException.forBucket(bucketName)
          : propagate(t);
    };
  }

  public CompletableFuture<Map<String, byte[]>> getAllBuckets(CoreCommonOptions options) {
    return httpClient.get(pathForBuckets(), options)
        .trace(TracingIdentifiers.SPAN_REQUEST_MB_GET_ALL_BUCKETS)
        .exec(core)
        .thenApply(response -> {
          JsonNode tree = Mapper.decodeIntoTree(response.content());
          Map<String, byte[]> out = new HashMap<>();
          for (final JsonNode bucket : tree) {
            String bucketName = requireNonNull(bucket.get("name").textValue(), "Bucket json is missing 'name' field: " + redactMeta(bucket));
            out.put(bucketName, Mapper.encodeAsBytes(bucket));
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
}
