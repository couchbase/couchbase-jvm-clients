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
import com.couchbase.client.core.error.BucketExistsException;
import com.couchbase.client.core.error.BucketNotFlushableException;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.manager.CoreBucketManager;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.AsyncCluster;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.couchbase.client.core.util.CbCollections.transformValues;
import static com.couchbase.client.core.util.CbThrowables.hasCause;
import static com.couchbase.client.core.util.CbThrowables.throwIfUnchecked;
import static com.couchbase.client.java.manager.bucket.BucketType.MEMCACHED;
import static com.couchbase.client.java.manager.bucket.CreateBucketOptions.createBucketOptions;
import static com.couchbase.client.java.manager.bucket.DropBucketOptions.dropBucketOptions;
import static com.couchbase.client.java.manager.bucket.FlushBucketOptions.flushBucketOptions;
import static com.couchbase.client.java.manager.bucket.GetAllBucketOptions.getAllBucketOptions;
import static com.couchbase.client.java.manager.bucket.GetBucketOptions.getBucketOptions;
import static com.couchbase.client.java.manager.bucket.UpdateBucketOptions.updateBucketOptions;

/**
 * Performs (async) management operations on Buckets.
 * <p>
 * All mutating operations on this manager are eventually consistent, which means that as soon as the call returns
 * the operation is accepted by the server, but it does not mean that the operation has been applied to all nodes
 * in the cluster yet. In the future, APIs will be provided which allow to assert the propagation state.
 */
public class AsyncBucketManager {

  /**
   * References the core-io bucket manager which abstracts common I/O functionality.
   */
  private final CoreBucketManager coreBucketManager;

  /**
   * Creates a new {@link AsyncBucketManager}.
   * <p>
   * This API is not intended to be called by the user directly, use {@link AsyncCluster#buckets()}
   * instead.
   *
   * @param core the internal core reference.
   */
  @Stability.Internal
  public AsyncBucketManager(final Core core) {
    this.coreBucketManager = new CoreBucketManager(core);
  }

  /**
   * Creates a new bucket on the server.
   * <p>
   * The SDK will not perform any logical validation on correct combination of the settings - the server will return
   * an error on invalid combinations. As an example, a magma bucket needs at least 1024 MiB of bucket quota - otherwise
   * the server will reject it.
   *
   * @param settings the {@link BucketSettings} describing the properties of the bucket.
   * @return a {@link CompletableFuture} completing when the operation is applied or has failed with an error.
   * @throws BucketExistsException (async) if the bucket already exists.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createBucket(final BucketSettings settings) {
    return createBucket(settings, createBucketOptions());
  }

  /**
   * Creates a new bucket on the server with custom options.
   * <p>
   * The SDK will not perform any logical validation on correct combination of the settings - the server will return
   * an error on invalid combinations. As an example, a magma bucket needs at least 1024 MiB of bucket quota - otherwise
   * the server will reject it.
   *
   * @param settings the {@link BucketSettings} describing the properties of the bucket.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or has failed with an error.
   * @throws BucketExistsException (async) if the bucket already exists.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createBucket(final BucketSettings settings, final CreateBucketOptions options) {
    CreateBucketOptions.Built bltOptions = options.build();

    CompletableFuture<Void> result = coreBucketManager.createBucket(toMap(settings), bltOptions);
    return result.exceptionally(t -> {
      if (bltOptions.ignoreIfExists() && hasCause(t, BucketExistsException.class)) {
        return null;
      }
      throwIfUnchecked(t);
      throw new RuntimeException(t);
    });
  }

  /**
   * Updates the settings of a bucket which already exists.
   * <p>
   * Not all properties of a bucket can be changed on an update. Notably, the following properties are ignored
   * by the SDK on update and so will not produce an error but also not change anything on the server side:
   * <ul>
   *   <li>{@link BucketSettings#name()}</li>
   *   <li>{@link BucketSettings#bucketType(BucketType)}</li>
   *   <li>{@link BucketSettings#conflictResolutionType(ConflictResolutionType)}</li>
   *   <li>{@link BucketSettings#replicaIndexes(boolean)}</li>
   *   <li>{@link BucketSettings#storageBackend(StorageBackend)}</li>
   * </ul>
   * <p>
   * The SDK will not perform any logical validation on correct combination of the settings - the server will return
   * an error on invalid combinations. As an example, a magma bucket needs at least 1024 MiB of bucket quota - otherwise
   * the server will reject it.
   *
   * @param settings the {@link BucketSettings} describing the properties of the bucket.
   * @return a {@link CompletableFuture} completing when the operation is applied or has failed with an error.
   * @throws BucketNotFoundException (async) if the specified bucket does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> updateBucket(final BucketSettings settings) {
    return updateBucket(settings, updateBucketOptions());
  }

  /**
   * Updates the settings of a bucket which already exists with custom options.
   * <p>
   * Not all properties of a bucket can be changed on an update. Notably, the following properties are ignored
   * by the SDK on update and so will not produce an error but also not change anything on the server side:
   * <ul>
   *   <li>{@link BucketSettings#name()}</li>
   *   <li>{@link BucketSettings#bucketType(BucketType)}</li>
   *   <li>{@link BucketSettings#conflictResolutionType(ConflictResolutionType)}</li>
   *   <li>{@link BucketSettings#replicaIndexes(boolean)}</li>
   *   <li>{@link BucketSettings#storageBackend(StorageBackend)}</li>
   * </ul>
   * <p>
   * The SDK will not perform any logical validation on correct combination of the settings - the server will return
   * an error on invalid combinations. As an example, a magma bucket needs at least 1024 MiB of bucket quota - otherwise
   * the server will reject it.
   *
   * @param settings the {@link BucketSettings} describing the properties of the bucket.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or has failed with an error.
   * @throws BucketNotFoundException (async) if the specified bucket does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> updateBucket(final BucketSettings settings, final UpdateBucketOptions options) {
    return coreBucketManager.updateBucket(toMap(settings), options.build());
  }

  /**
   * Drops ("deletes") a bucket from the cluster.
   *
   * @param bucketName the name of the bucket to drop.
   * @return a {@link CompletableFuture} completing when the operation is applied or has failed with an error.
   * @throws BucketNotFoundException (async) if the specified bucket does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropBucket(final String bucketName) {
    return dropBucket(bucketName, dropBucketOptions());
  }

  /**
   * Drops ("deletes") a bucket from the cluster with custom options.
   *
   * @param bucketName the name of the bucket to drop.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or has failed with an error.
   * @throws BucketNotFoundException (async) if the specified bucket does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropBucket(final String bucketName, final DropBucketOptions options) {
    DropBucketOptions.Built bltOptions = options.build();
    return coreBucketManager.dropBucket(bucketName, bltOptions)
      .exceptionally(t -> {
        if (bltOptions.ignoreIfNotExists() && hasCause(t, BucketNotFoundException.class)) {
          return null;
        }
        throwIfUnchecked(t);
        throw new RuntimeException(t);
      });
  }

  /**
   * Loads the properties of a bucket from the cluster.
   *
   * @param bucketName the name of the bucket for which the settings should be loaded.
   * @return a {@link CompletableFuture} completing with the bucket settings or if the operation has failed.
   * @throws BucketNotFoundException (async) if the specified bucket does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<BucketSettings> getBucket(final String bucketName) {
    return getBucket(bucketName, getBucketOptions());
  }

  /**
   * Loads the properties of a bucket from the cluster with custom options.
   *
   * @param bucketName the name of the bucket for which the settings should be loaded.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing with the bucket settings or if the operation has failed.
   * @throws BucketNotFoundException (async) if the specified bucket does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<BucketSettings> getBucket(final String bucketName, final GetBucketOptions options) {
    return coreBucketManager.getBucket(bucketName, options.build())
        .thenApply(parseBucketSettings());
  }

  /**
   * Loads the properties of all buckets the current user has access to from the cluster.
   *
   * @return a {@link CompletableFuture} completing with all bucket settings or if the operation has failed.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Map<String, BucketSettings>> getAllBuckets() {
    return getAllBuckets(getAllBucketOptions());
  }

  /**
   * Loads the properties of all buckets the current user has access to from the cluster.
   *
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing with all bucket settings or if the operation has failed.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Map<String, BucketSettings>> getAllBuckets(final GetAllBucketOptions options) {
    return coreBucketManager
        .getAllBuckets(options.build())
        .thenApply(bucketNameToBytes -> transformValues(bucketNameToBytes, parseBucketSettings()));
  }

  /**
   * Deletes all documents from ("flushes") a bucket.
   * <p>
   * Flush needs to be enabled on the bucket in order to perform the operation. Enabling flush is not recommended
   * in a production cluster and can lead to data loss!
   * <p>
   * Keep in mind that flush is not an atomic operation, the server will need some time to clean the partitions
   * out completely. If isolation is preferred in an integration-test scenario, creating individual buckets might
   * provide a better user experience.
   *
   * @param bucketName the name of the bucket to flush.
   * @return a {@link CompletableFuture} completing when the operation is applied or has failed with an error.
   * @throws BucketNotFoundException (async) if the specified bucket does not exist.
   * @throws BucketNotFlushableException (async) if flush is not enabled on the bucket.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> flushBucket(final String bucketName) {
    return flushBucket(bucketName, flushBucketOptions());
  }

  /**
   * Deletes all documents from ("flushes") a bucket with custom options.
   * <p>
   * Flush needs to be enabled on the bucket in order to perform the operation. Enabling flush is not recommended
   * in a production cluster and can lead to data loss!
   * <p>
   * Keep in mind that flush is not an atomic operation, the server will need some time to clean the partitions
   * out completely. If isolation is preferred in an integration-test scenario, creating individual buckets might
   * provide a better user experience.
   *
   * @param bucketName the name of the bucket to flush.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or has failed with an error.
   * @throws BucketNotFoundException (async) if the specified bucket does not exist.
   * @throws BucketNotFlushableException (async) if flush is not enabled on the bucket.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> flushBucket(final String bucketName, final FlushBucketOptions options) {
    return coreBucketManager.flushBucket(bucketName, options.build());
  }

  /**
   * Returns a function that turns raw encoded bytes into {@link BucketSettings}.
   */
  private static Function<byte[], BucketSettings> parseBucketSettings() {
    return bucketBytes -> BucketSettings.create(Mapper.decodeIntoTree(bucketBytes));
  }

  /**
   * Turns {@link BucketSettings} into a map that represents the wire format of the server.
   *
   * @param settings the settings to encode.
   * @return the encoded settings in a map.
   */
  private static Map<String, String> toMap(final BucketSettings settings) {
    Map<String, String> params = new HashMap<>();

    params.put("ramQuotaMB", String.valueOf(settings.ramQuotaMB()));
    if (settings.bucketType() != MEMCACHED) {
      params.put("replicaNumber", String.valueOf(settings.numReplicas()));
    }
    params.put("flushEnabled", String.valueOf(settings.flushEnabled() ? 1 : 0));
    long maxTTL = settings.maxExpiry().getSeconds();
    // Do not send if it's been left at default, else will get an error on CE
    if (maxTTL != 0) {
      params.put("maxTTL", String.valueOf(maxTTL));
    }
    if (settings.evictionPolicy() != null) {
      // let server assign the default policy for this bucket type
      params.put("evictionPolicy", settings.evictionPolicy().alias());
    }
    // Do not send if it's been left at default, else will get an error on CE
    if (settings.compressionMode != null) {
      params.put("compressionMode", settings.compressionMode().alias());
    }

    if (settings.minimumDurabilityLevel() != DurabilityLevel.NONE) {
      params.put("durabilityMinLevel", settings.minimumDurabilityLevel().encodeForManagementApi());
    }
    if(settings.storageBackend() != null) {
      params.put("storageBackend",settings.storageBackend().alias());
    }

    params.put("name", settings.name());
    params.put("bucketType", settings.bucketType().alias());
    params.put("conflictResolutionType", settings.conflictResolutionType().alias());
    if (settings.bucketType() != BucketType.EPHEMERAL) {
      params.put("replicaIndex", String.valueOf(settings.replicaIndexes() ? 1 : 0));
    }

    return params;
  }

}
