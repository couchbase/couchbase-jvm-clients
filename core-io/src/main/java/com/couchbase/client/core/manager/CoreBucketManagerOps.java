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

package com.couchbase.client.core.manager;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.manager.bucket.CoreBucketSettings;
import com.couchbase.client.core.manager.bucket.CoreCreateBucketSettings;
import reactor.util.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Stability.Internal
public interface CoreBucketManagerOps {
  CompletableFuture<Void> createBucket(CoreBucketSettings settings, @Nullable CoreCreateBucketSettings createSpecificSettings, CoreCommonOptions options);

  CompletableFuture<Void> updateBucket(CoreBucketSettings settings, CoreCommonOptions options);

  CompletableFuture<Void> dropBucket(String bucketName, CoreCommonOptions options);

  CompletableFuture<CoreBucketSettings> getBucket(String bucketName, CoreCommonOptions options);

  CompletableFuture<Map<String, CoreBucketSettings>> getAllBuckets(CoreCommonOptions options);

  CompletableFuture<Void> flushBucket(String bucketName, CoreCommonOptions options);
}

