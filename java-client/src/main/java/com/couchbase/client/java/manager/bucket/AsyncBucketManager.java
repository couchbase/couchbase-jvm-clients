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

import com.couchbase.client.core.CoreProtostellar;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.CoreCouchbaseOps;
import com.couchbase.client.core.error.BucketExistsException;
import com.couchbase.client.core.error.BucketNotFlushableException;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.manager.CoreBucketManagerOps;
import com.couchbase.client.core.util.PreventsGarbageCollection;
import com.couchbase.client.java.AsyncCluster;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.util.CbCollections.transformValues;
import static com.couchbase.client.java.manager.bucket.CreateBucketOptions.createBucketOptions;
import static com.couchbase.client.java.manager.bucket.DropBucketOptions.dropBucketOptions;
import static com.couchbase.client.java.manager.bucket.FlushBucketOptions.flushBucketOptions;
import static com.couchbase.client.java.manager.bucket.GetAllBucketOptions.getAllBucketOptions;
import static com.couchbase.client.java.manager.bucket.GetBucketOptions.getBucketOptions;
import static com.couchbase.client.java.manager.bucket.UpdateBucketOptions.updateBucketOptions;
import static java.util.Objects.requireNonNull;

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
  private final CoreBucketManagerOps coreBucketManager;

  @PreventsGarbageCollection
  private final AsyncCluster cluster;

  private final boolean isProtostellar;

  /**
   * Creates a new {@link AsyncBucketManager}.
   * <p>
   * This API is not intended to be called by the user directly, use {@link AsyncCluster#buckets()}
   * instead.
   */
  @Stability.Internal
  public AsyncBucketManager(
    final CoreCouchbaseOps ops,
    final AsyncCluster cluster
  ) {
    isProtostellar = (ops instanceof CoreProtostellar);
    this.coreBucketManager = ops.bucketManager();
    this.cluster = requireNonNull(cluster);
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
    // JVMCBC-1395: Ideally we only be sending settings the user has specified.
    // But for Classic we have always sent some fields (those in BucketSettings.createDefaults), and cannot change this
    // without potential breakage.
    // For Protostellar we have a clean slate to implement things correctly.
    BucketSettings merged;
    if (isProtostellar) {
      merged = settings;
    }
    else {
      BucketSettings base = BucketSettings.createDefaults(settings.name());
      merged = BucketSettings.merge(base, settings);
    }

    return coreBucketManager.createBucket(merged.toCore(), options.build());
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
    return coreBucketManager.updateBucket(settings.toCore(), options.build());
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
    return coreBucketManager.dropBucket(bucketName, options.build());
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
    return coreBucketManager.getBucket(bucketName, options.build()).thenApply(BucketSettings::new);
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
        .thenApply(buckets -> transformValues(buckets, BucketSettings::new));
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
}
