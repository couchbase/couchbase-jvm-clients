/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.client.java.manager.bucket;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.util.UrlQueryStringBuilder;
import com.couchbase.client.java.manager.ManagerSupport;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.DELETE;
import static com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.GET;
import static com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.POST;
import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode;
import static com.couchbase.client.java.manager.bucket.CreateBucketOptions.createBucketOptions;
import static com.couchbase.client.java.manager.bucket.DropBucketOptions.dropBucketOptions;
import static com.couchbase.client.java.manager.bucket.FlushBucketOptions.flushBucketOptions;
import static com.couchbase.client.java.manager.bucket.GetAllBucketOptions.getAllBucketOptions;
import static com.couchbase.client.java.manager.bucket.GetBucketOptions.getBucketOptions;
import static com.couchbase.client.java.manager.bucket.UpdateBucketOptions.updateBucketOptions;

@Stability.Volatile
public class AsyncBucketManager extends ManagerSupport {

  public AsyncBucketManager(final Core core) {
    super(core);
  }

  private static String pathForBuckets() {
    return "/pools/default/buckets/";
  }

  private static String pathForBucket(final String bucketName) {
    return pathForBuckets() + urlEncode(bucketName);
  }

  private static String pathForBucketFlush(final String bucketName) {
    return "/pools/default/buckets/" + urlEncode(bucketName) + "/controller/doFlush";
  }

  public CompletableFuture<Void> createBucket(final BucketSettings settings) {
    return createBucket(settings, createBucketOptions());
  }

  public CompletableFuture<Void> createBucket(final BucketSettings settings, final CreateBucketOptions options) {
    return sendRequest(POST, pathForBuckets(), convertSettingsToParams(settings, false)).thenApply(response -> {
      if (response.status() == ResponseStatus.INVALID_ARGS && response.content() != null) {
        String content = new String(response.content(), StandardCharsets.UTF_8);
        if (content.contains("Bucket with given name already exists")) {
          throw BucketAlreadyExistsException.forBucket(settings.name());
        }
        else {
          throw new CouchbaseException(content);
        }
      }
      checkStatus(response, "create bucket [" + redactMeta(settings) + "]");
      return null;
    });
  }

  public CompletableFuture<Void> updateBucket(final BucketSettings settings) {
    return updateBucket(settings, updateBucketOptions());
  }

  public CompletableFuture<Void> updateBucket(final BucketSettings settings, final UpdateBucketOptions options) {
    return Mono
      .fromFuture(this::getAllBuckets)
      .map(buckets -> buckets.containsKey(settings.name()))
      .flatMap(bucketExists -> {
        if (!bucketExists) {
          return Mono.error(BucketNotFoundException.forBucket(settings.name()));
        }
        return Mono.fromFuture(sendRequest(POST, pathForBucket(settings.name()), convertSettingsToParams(settings, true)).thenApply(response -> {
          checkStatus(response, "update bucket [" + redactMeta(settings) + "]");
          return null;
        }));
      })
      .then()
      .toFuture();
  }

  private UrlQueryStringBuilder convertSettingsToParams(final BucketSettings settings, boolean update) {
    UrlQueryStringBuilder params = UrlQueryStringBuilder.createForUrlSafeNames();

    params.add("ramQuotaMB", settings.ramQuotaMB());
    params.add("replicaNumber", settings.numReplicas());
    params.add("flushEnabled", settings.flushEnabled() ? 1 : 0);
    params.add("maxTTL", settings.maxTTL());
    params.add("evictionPolicy", settings.ejectionPolicy().alias());
    params.add("compressionMode", settings.compressionMode().alias());

    // The following values must not be changed on update
    if (!update) {
      params.add("name", settings.name());
      params.add("bucketType", settings.bucketType().alias());
      params.add("conflictResolutionType", settings.conflictResolutionType().alias());
      params.add("replicaIndex", settings.replicaIndexes() ? 1 : 0);
    }

    return params;
  }

  public CompletableFuture<Void> dropBucket(final String bucketName) {
    return dropBucket(bucketName, dropBucketOptions());
  }

  public CompletableFuture<Void> dropBucket(final String bucketName, final DropBucketOptions options) {
    return sendRequest(DELETE, pathForBucket(bucketName)).thenApply(response -> {
      if (response.status() == ResponseStatus.NOT_FOUND) {
        throw BucketNotFoundException.forBucket(bucketName);
      }
      checkStatus(response, "drop bucket [" + redactMeta(bucketName) + "]");
      return null;
    });
  }

  public CompletableFuture<BucketSettings> getBucket(final String bucketName) {
    return getBucket(bucketName, getBucketOptions());
  }

  public CompletableFuture<BucketSettings> getBucket(final String bucketName, final GetBucketOptions options) {
    return sendRequest(GET, pathForBucket(bucketName)).thenApply(response -> {
      if (response.status() == ResponseStatus.NOT_FOUND) {
        throw BucketNotFoundException.forBucket(bucketName);
      }
      checkStatus(response, "get bucket [" + redactMeta(bucketName) + "]");
      return Mapper.decodeInto(response.content(), BucketSettings.class);
    });
  }

  public CompletableFuture<Map<String, BucketSettings>> getAllBuckets() {
    return getAllBuckets(getAllBucketOptions());
  }

  public CompletableFuture<Map<String, BucketSettings>> getAllBuckets(final GetAllBucketOptions options) {
    return sendRequest(GET, pathForBuckets()).thenApply(response -> {
      checkStatus(response, "get all buckets");
      return Mapper
        .decodeInto(response.content(), new TypeReference<List<BucketSettings>>() {})
        .stream()
        .collect(Collectors.toMap(BucketSettings::name, bs -> bs));
    });
  }

  public CompletableFuture<Void> flushBucket(final String bucketName) {
    return flushBucket(bucketName, flushBucketOptions());
  }

  public CompletableFuture<Void> flushBucket(final String bucketName, final FlushBucketOptions options) {
    return sendRequest(POST, pathForBucketFlush(bucketName)).thenApply(response -> {
      if (response.status() == ResponseStatus.NOT_FOUND) {
        throw BucketNotFoundException.forBucket(bucketName);
      }
      checkStatus(response, "flush bucket [" + redactMeta(bucketName) + "]");
      return null;
    });
  }

}
