/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.service.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.Optional;

public class ObserveContext extends CoreContext {

  private final Observe.ObservePersistTo persistTo;
  private final Observe.ObserveReplicateTo replicateTo;
  private final Optional<MutationToken> mutationToken;
  private final long cas;
  private final String bucket;
  private final String key;
  private final byte[] collection;
  private final boolean remove;
  private final Duration timeout;
  private final RetryStrategy retryStrategy;

  public ObserveContext(CoreContext ctx, Observe.ObservePersistTo persistTo, Observe.ObserveReplicateTo replicateTo,
                        Optional<MutationToken> mutationToken, long cas, String bucket, String key,
                        byte[] collection, boolean remove, Duration timeout) {
    super(ctx.core(), ctx.id(), ctx.environment());
    this.persistTo = persistTo;
    this.replicateTo = replicateTo;
    this.mutationToken = mutationToken;
    this.cas = cas;
    this.bucket = bucket;
    this.key = key;
    this.collection = collection;
    this.remove = remove;
    this.timeout = timeout;
    this.retryStrategy = FailFastRetryStrategy.INSTANCE;
  }

  public Observe.ObservePersistTo persistTo() {
    return persistTo;
  }

  public Observe.ObserveReplicateTo replicateTo() {
    return replicateTo;
  }

  public Optional<MutationToken> mutationToken() {
    return mutationToken;
  }

  public long cas() {
    return cas;
  }

  public String bucket() {
    return bucket;
  }

  public String key() {
    return key;
  }

  public byte[] collection() {
    return collection;
  }

  public boolean remove() {
    return remove;
  }

  public Duration timeout() {
    return timeout;
  }

  public RetryStrategy retryStrategy() {
    return retryStrategy;
  }
}
