/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.manager.bucket;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.config.BucketType;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import reactor.util.annotation.Nullable;

import java.time.Duration;

@Stability.Internal
public interface CoreBucketSettings {
  String name();

  @Nullable
  Boolean flushEnabled();

  long ramQuotaMB();

  @Nullable
  Integer numReplicas();

  @Nullable
  Boolean replicaIndexes();

  @Nullable
  BucketType bucketType();

  @Nullable
  CoreEvictionPolicyType evictionPolicy();

  @Nullable
  Duration maxExpiry();

  @Nullable
  CoreCompressionMode compressionMode();

  @Nullable
  DurabilityLevel minimumDurabilityLevel();

  @Nullable
  CoreStorageBackend storageBackend();
}
