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

package com.couchbase.client.core.io;

import com.couchbase.client.core.util.UnsignedLEB128;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The {@link CollectionMap} maps a locator to the encoded collection ID representation.
 */
public class CollectionMap {

  /**
   * Holds the actual inner map.
   */
  private final ConcurrentHashMap<CollectionIdentifier, byte[]> inner = new ConcurrentHashMap<>();

  /**
   * Holds the identifier for the default collection.
   */
  private static final byte[] DEFAULT_ID = UnsignedLEB128.encode(0);

  /**
   * Retrieves the collection id for the given identifier.
   *
   * Might return null if not found! Also it will return the default id for the default scope/collection.
   *
   * @param key the key to check
   * @return the collection id.
   */
  public byte[] get(final CollectionIdentifier key) {
    if (key.isDefault()) {
      return DEFAULT_ID;
    }
    return inner.get(key);
  }

  /**
   * Stores a new collection ID with the given identifier.
   *
   * @param key the key to store.
   * @param value the value associated.
   */
  public void put(final CollectionIdentifier key, byte[] value) {
    inner.put(key, value);
  }

  /**
   * Checks if the given bucket is at all present in the map.
   *
   * @param bucket the bucket name to check
   * @return true if so, false otherwise.
   */
  public boolean hasBucketMap(final String bucket) {
    for (CollectionIdentifier identifier : inner.keySet()) {
      if (bucket.equals(identifier.bucket())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the inner map, mainly for print/debug purposes.
   *
   * @return the inner map, immutable.
   */
  public Map<CollectionIdentifier, byte[]> inner() {
    return Collections.unmodifiableMap(inner);
  }
}
