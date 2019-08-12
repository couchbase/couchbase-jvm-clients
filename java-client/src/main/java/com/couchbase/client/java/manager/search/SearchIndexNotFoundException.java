/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.client.java.manager.search;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CouchbaseException;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static java.util.Objects.requireNonNull;

@Stability.Volatile
public class SearchIndexNotFoundException extends CouchbaseException {
  private final String indexName;

  public SearchIndexNotFoundException(String indexName) {
    super("Search Index [" + redactSystem(indexName) + "] not found.");
    this.indexName = requireNonNull(indexName);
  }

  public static SearchIndexNotFoundException forIndex(String indexName) {
    return new SearchIndexNotFoundException(indexName);
  }

  public String indexName() {
    return indexName;
  }
}
