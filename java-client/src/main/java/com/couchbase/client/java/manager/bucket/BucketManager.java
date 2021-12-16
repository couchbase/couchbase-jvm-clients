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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.BucketExistsException;
import com.couchbase.client.core.error.BucketNotFlushableException;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.Cluster;

import java.util.Map;

import static com.couchbase.client.java.AsyncUtils.block;
import static java.util.Objects.requireNonNull;

/**
 * Performs management operations on Buckets.
 * <p>
 * All mutating operations on this manager are eventually consistent, which means that as soon as the call returns
 * the operation is accepted by the server, but it does not mean that the operation has been applied to all nodes
 * in the cluster yet. In the future, APIs will be provided which allow to assert the propagation state.
 */
public class BucketManager {

  /**
   * Holds the underlying async bucket manager.
   */
  private final AsyncBucketManager async;

  /**
   * Provides access to the reactive bucket manager for convenience.
   */
  private final ReactiveBucketManager reactive;

  /**
   * Creates a new {@link BucketManager}.
   * <p>
   * This API is not intended to be called by the user directly, use {@link Cluster#buckets()}
   * instead.
   *
   * @param async the underlying async manager that performs the ops.
   */
  @Stability.Internal
  public BucketManager(final AsyncBucketManager async) {
    this.async = requireNonNull(async);
    this.reactive = new ReactiveBucketManager(async);
  }

  /**
   * Provides access to the {@link AsyncBucketManager} for convenience.
   */
  public AsyncBucketManager async() {
    return async;
  }

  /**
   * Provides access to the {@link ReactiveBucketManager} for convenience.
   */
  public ReactiveBucketManager reactive() {
    return reactive;
  }

  /**
   * Creates a new bucket on the server.
   * <p>
   * The SDK will not perform any logical validation on correct combination of the settings - the server will return
   * an error on invalid combinations. As an example, a magma bucket needs at least 256mb of bucket quota - otherwise
   * the server will reject it.
   *
   * @param settings the {@link BucketSettings} describing the properties of the bucket.
   * @throws BucketExistsException if the bucket already exists.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void createBucket(final BucketSettings settings) {
    block(async.createBucket(settings));
  }

  /**
   * Creates a new bucket on the server with custom options.
   * <p>
   * The SDK will not perform any logical validation on correct combination of the settings - the server will return
   * an error on invalid combinations. As an example, a magma bucket needs at least 256mb of bucket quota - otherwise
   * the server will reject it.
   *
   * @param settings the {@link BucketSettings} describing the properties of the bucket.
   * @param options the custom options to apply.
   * @throws BucketExistsException if the bucket already exists.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void createBucket(final BucketSettings settings, final CreateBucketOptions options) {
    block(async.createBucket(settings, options));
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
   * an error on invalid combinations. As an example, a magma bucket needs at least 256mb of bucket quota - otherwise
   * the server will reject it.
   *
   * @param settings the {@link BucketSettings} describing the properties of the bucket.
   * @throws BucketNotFoundException if the specified bucket does not exist.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void updateBucket(final BucketSettings settings) {
    block(async.updateBucket(settings));
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
   * an error on invalid combinations. As an example, a magma bucket needs at least 256mb of bucket quota - otherwise
   * the server will reject it.
   *
   * @param settings the {@link BucketSettings} describing the properties of the bucket.
   * @param options the custom options to apply.
   * @throws BucketNotFoundException if the specified bucket does not exist.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void updateBucket(final BucketSettings settings, final UpdateBucketOptions options) {
    block(async.updateBucket(settings, options));
  }

  /**
   * Drops ("deletes") a bucket from the cluster.
   *
   * @param bucketName the name of the bucket to drop.
   * @throws BucketNotFoundException if the specified bucket does not exist.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void dropBucket(final String bucketName) {
    block(async.dropBucket(bucketName));
  }

  /**
   * Drops ("deletes") a bucket from the cluster with custom options.
   *
   * @param bucketName the name of the bucket to drop.
   * @param options the custom options to apply.
   * @throws BucketNotFoundException if the specified bucket does not exist.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void dropBucket(final String bucketName, final DropBucketOptions options) {
    block(async.dropBucket(bucketName, options));
  }

  /**
   * Loads the properties of a bucket from the cluster.
   *
   * @param bucketName the name of the bucket for which the settings should be loaded.
   * @return the bucket settings when the operation completed successfully.
   * @throws BucketNotFoundException if the specified bucket does not exist.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public BucketSettings getBucket(final String bucketName) {
    return block(async.getBucket(bucketName));
  }

  /**
   * Loads the properties of a bucket from the cluster with custom options.
   *
   * @param bucketName the name of the bucket for which the settings should be loaded.
   * @param options the custom options to apply.
   * @return the bucket settings when the operation completed successfully.
   * @throws BucketNotFoundException if the specified bucket does not exist.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public BucketSettings getBucket(final String bucketName, final GetBucketOptions options) {
    return block(async.getBucket(bucketName, options));
  }

  /**
   * Loads the properties of all buckets the current user has access to from the cluster.
   *
   * @return the bucket settings when the operation completed successfully.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public Map<String, BucketSettings> getAllBuckets() {
    return block(async.getAllBuckets());
  }

  /**
   * Loads the properties of all buckets the current user has access to from the cluster.
   *
   * @param options the custom options to apply.
   * @return the bucket settings when the operation completed successfully.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public Map<String, BucketSettings> getAllBuckets(final GetAllBucketOptions options) {
    return block(async.getAllBuckets(options));
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
   * @throws BucketNotFoundException if the specified bucket does not exist.
   * @throws BucketNotFlushableException if flush is not enabled on the bucket.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void flushBucket(final String bucketName) {
    block(async.flushBucket(bucketName));
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
   * @throws BucketNotFoundException if the specified bucket does not exist.
   * @throws BucketNotFlushableException if flush is not enabled on the bucket.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void flushBucket(final String bucketName, final FlushBucketOptions options) {
    block(async.flushBucket(bucketName, options));
  }

}
