/*
 * Copyright 2022 Couchbase, Inc.
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

package com.couchbase.client.core.manager;

import com.couchbase.client.core.annotation.Stability;
import reactor.util.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_COLLECTION;

@Stability.Internal
public class CoreQueryIndexManager {

  public static Map<String, String> getNamedParamsForGetAllIndexes(
      String bucket,
      @Nullable String scope,
      @Nullable String collection
  ) {
    Map<String, String> params = new HashMap<>();
    params.put("bucketName", bucket);
    params.put("scopeName", scope);
    params.put("collectionName", collection);
    return params;
  }

  public static String getStatementForGetAllIndexes(
      String bucket,
      @Nullable String scope,
      @Nullable String collection) {

    if (collection != null && scope == null) {
      throw new IllegalArgumentException("When collection is non-null, scope must also be non-null.");
    }

    String bucketCondition = "(bucket_id = $bucketName)";
    String scopeCondition = "(" + bucketCondition + " AND scope_id = $scopeName)";
    String collectionCondition = "(" + scopeCondition + " AND keyspace_id = $collectionName)";

    String whereCondition;
    if (collection != null) {
      whereCondition = collectionCondition;
    } else if (scope != null) {
      whereCondition = scopeCondition;
    } else {
      whereCondition = bucketCondition;
    }

    // If indexes on the default collection should be included in the results,
    // modify the query to match the irregular structure of those indexes.
    if (DEFAULT_COLLECTION.equals(collection) || collection == null) {
      String defaultCollectionCondition = "(bucket_id IS MISSING AND keyspace_id = $bucketName)";
      whereCondition = "(" + whereCondition + " OR " + defaultCollectionCondition + ")";
    }

    return "SELECT idx.* FROM system:indexes AS idx" +
        " WHERE " + whereCondition +
        " AND `using` = \"gsi\"" +
        " ORDER BY is_primary DESC, name ASC";
  }

}
