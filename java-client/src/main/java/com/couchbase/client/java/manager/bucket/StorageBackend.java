/*
 * Copyright 2021 Couchbase, Inc.
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

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonValue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

public class StorageBackend {
  private static final ConcurrentMap<String, StorageBackend> interned = new ConcurrentHashMap<>();

  public static final StorageBackend COUCHSTORE = of("couchstore");
  public static final StorageBackend MAGMA = of("magma");

  private final String alias;

  private StorageBackend(String alias) {
    this.alias = requireNonNull(alias);
  }

  @JsonCreator
  public static StorageBackend of(String alias) {
    return interned.computeIfAbsent(alias, k -> new StorageBackend(alias));
  }

  @JsonValue
  public String alias() {
    return alias;
  }

  @Override
  public String toString() {
    return alias();
  }
}
