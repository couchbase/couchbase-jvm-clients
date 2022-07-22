/*
 * Copyright (c) 2022 Couchbase, Inc.
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

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.retry.RetryStrategy;
import java.time.Duration;

/**
 * Parent class for requests which have their partition already set at creation time.
 */
public abstract class PredeterminedPartitionRequest<R extends Response> extends BaseKeyValueRequest<R> {
  private final short partition;

  public PredeterminedPartitionRequest(short partition, Duration timeout, CoreContext ctx, RetryStrategy retryStrategy,
                                       String key, CollectionIdentifier collectionIdentifier, RequestSpan span) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier, span);
    this.partition = partition;
  }

  @Override
  public short partition() {
    return partition;
  }

  @Override
  public void partition(short partition) {
    throw new UnsupportedOperationException("This request targets a specific partition; the partition " +
      "cannot be reassigned.");
  }

}