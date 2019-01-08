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
import com.couchbase.client.java.kv.DecrementOptions;
import com.couchbase.client.java.kv.IncrementOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PrependOptions;

import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

public class AsyncBinaryCollection {

  public CompletableFuture<MutationResult> append(final String id, final byte[] content) {
    return append(id, content, AppendOptions.DEFAULT);
  }

  public CompletableFuture<MutationResult> append(final String id, final byte[] content,
                                                  final AppendOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(content, "Content");
    notNull(options, "AppendOptions");

    return null;
  }

  public CompletableFuture<MutationResult> prepend(final String id, final byte[] content) {
    return prepend(id, content, PrependOptions.DEFAULT);
  }

  public CompletableFuture<MutationResult> prepend(final String id, final byte[] content,
                                                   final PrependOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(content, "Content");
    notNull(options, "PrependOptions");

    return null;
  }

  public CompletableFuture<MutationResult> increment(final String id) {
    return increment(id, IncrementOptions.DEFAULT);
  }

  public CompletableFuture<MutationResult> increment(final String id, final IncrementOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(options, "IncrementOptions");

    return null;
  }

  public CompletableFuture<MutationResult> decrement(final String id) {
    return decrement(id, DecrementOptions.DEFAULT);
  }

  public CompletableFuture<MutationResult> decrement(final String id, final DecrementOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(options, "DecrementOptions");

    return null;
  }
}
