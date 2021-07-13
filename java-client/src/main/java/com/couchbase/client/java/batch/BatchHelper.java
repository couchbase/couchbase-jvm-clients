/*
 * Copyright (c) 2020 Couchbase, Inc.
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

package com.couchbase.client.java.batch;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.kv.GetResult;

import java.util.List;
import java.util.Map;

/**
 * This helper class provides methods that make performing batch operations easy and comfortable.
 */
@Stability.Uncommitted
public class BatchHelper {

  private BatchHelper() {}

  /**
   * First checks if the given IDs exist and if so fetches their contents.
   * <p>
   * Please take into consideration when using this API that it only makes sense to use it if of the many ids provided
   * only a small subset comes back. (So let's say you give it 1000 IDs but you only expect 50 to be there or so).
   * Otherwise if all are most of them are there, just use a bulk get with the reactive API directly - you won't see
   * much benefit in this case.
   *
   * @param collection the collection to perform the fetch on.
   * @param ids the document IDs to fetch.
   * @return a Map of the document IDs as the key and the result (if found).
   */
  @Stability.Uncommitted
  public static Map<String, GetResult> getIfExists(final Collection collection,
                                                   final java.util.Collection<String> ids) {
    return ReactiveBatchHelper.getIfExists(collection, ids).block();
  }

  /**
   * Returns a list of ids for the documents that exist.
   * <p>
   * Note that you usually only want to use this API if you really need to bulk check exists on many documents
   * at once, for all other use cases we recommend trying the regular, supported APIs first
   * (i.e. using {@link com.couchbase.client.java.ReactiveCollection#exists(String)}).
   *
   * @param collection the collection to perform the exists checks on.
   * @param ids the document IDs to check.
   * @return a list of all the ids that are found.
   */
  @Stability.Uncommitted
  public static List<String> exists(final Collection collection, final java.util.Collection<String> ids) {
    return ReactiveBatchHelper.exists(collection, ids).collectList().block();
  }

}
