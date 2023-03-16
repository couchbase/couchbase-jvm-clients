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
package com.couchbase.client.core.api.kv;

import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.error.context.ErrorContext;
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext;

import java.util.Optional;
import java.util.function.Supplier;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

public class CoreKvBinaryParamValidators {

  public static void validateAppendPrependArgs(String key, CoreKeyspace keyspace, CoreCommonOptions options,
                                               byte[] content, long cas, CoreDurability durability) {
    Supplier<ErrorContext> supplier = () -> ReducedKeyValueErrorContext.create(key, keyspace.toCollectionIdentifier());
    notNullOrEmpty(key, "Id", supplier);
    notNull(keyspace, "Keyspace", supplier);
    notNull(options, "Options", supplier);
    notNull(content, "Content", supplier);
    notNull(durability, "Durability", supplier);
  }

  public static void validateIncrementDecrementArgs(String key, CoreKeyspace keyspace,
                                                    CoreCommonOptions options, CoreExpiry expiry, long delta, Optional<Long> initial, CoreDurability durability) {
    Supplier<ErrorContext> supplier = () -> ReducedKeyValueErrorContext.create(key, keyspace.toCollectionIdentifier());
    notNullOrEmpty(key, "Id", supplier);
    notNull(keyspace, "Keyspace", supplier);
    notNull(options, "Options", supplier);
    notNull(initial, "Initial", supplier);
    notNull(durability, "Durability", supplier);
    notNull(expiry, "Expiry", supplier);
  }

}
