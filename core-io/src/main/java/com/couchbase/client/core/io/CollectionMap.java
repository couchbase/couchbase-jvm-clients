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

import java.util.concurrent.ConcurrentHashMap;

/**
 * The {@link CollectionMap} maps a locator to the encoded collection ID representation.
 */
public class CollectionMap extends ConcurrentHashMap<CollectionIdentifier, byte[]> {

  /**
   * Checks if the given bucket is at all present in the map.
   *
   * @param bucket the bucket name to check
   * @return true if so, false otherwise.
   */
  public boolean hasBucketMap(final String bucket) {
    for (CollectionIdentifier identifier : keySet()) {
      if (bucket.equals(identifier.bucket())) {
        return true;
      }
    }
    return false;
  }

}
