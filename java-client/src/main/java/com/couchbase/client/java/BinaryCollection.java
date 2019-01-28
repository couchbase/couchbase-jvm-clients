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

package com.couchbase.client.java;

import com.couchbase.client.java.kv.AppendOptions;
import com.couchbase.client.java.kv.CounterResult;
import com.couchbase.client.java.kv.DecrementOptions;
import com.couchbase.client.java.kv.IncrementOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PrependOptions;

import static com.couchbase.client.java.AsyncUtils.block;

public class BinaryCollection {

  private final AsyncBinaryCollection async;

  BinaryCollection(AsyncBinaryCollection asyncBinaryCollection) {
    this.async = asyncBinaryCollection;
  }

  public MutationResult append(final String id, final byte[] content) {
    return block(async.append(id, content));
  }

  public MutationResult append(final String id, final byte[] content,
                               final AppendOptions options) {
    return block(async.append(id, content, options));
  }

  public MutationResult prepend(final String id, final byte[] content) {
    return block(async.prepend(id, content));
  }

  public MutationResult prepend(final String id, final byte[] content,
                                final PrependOptions options) {
    return block(async.prepend(id, content, options));
  }

  public CounterResult increment(final String id) {
    return block(async.increment(id));
  }

  public CounterResult increment(final String id, final IncrementOptions options) {
    return block(async.increment(id, options));
  }

  public CounterResult decrement(final String id) {
    return block(async.decrement(id));
  }

  public CounterResult decrement(final String id, final DecrementOptions options) {
    return block(async.decrement(id, options));
  }

}
