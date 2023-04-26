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

package com.couchbase.client.core.api.search.sort;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.TextNode;
import com.couchbase.client.protostellar.search.v1.Sorting;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.unsupportedInProtostellar;
import static com.couchbase.client.core.util.Validators.notNull;

/**
 * A sort tier specified as a string. Syntax is defined by
 * <a href="https://docs.couchbase.com/server/current/fts/fts-search-response.html#sorting-with-strings">
 * Sorting with Strings
 * </a>.
 */
@Stability.Internal
public class CoreSearchSortString extends CoreSearchSort {
  private final String sortString;

  public CoreSearchSortString(String sortString) {
    super(null); // "descending" is not applicable
    this.sortString = notNull(sortString, "sort string");
  }

  @Override
  public JsonNode toJsonNode() {
    return new TextNode(sortString);
  }

  @Override
  protected void injectParams(final ObjectNode queryJson) {
    // The sort string is passed as a simple JSON string, not an Object.
    throw new UnsupportedOperationException();
  }

  @Override
  protected String identifier() {
    return "N/A";
  }

  @Override
  public Sorting asProtostellar() {
    // Requires ING-381
    throw unsupportedInProtostellar("Specifying FTS sort order using a string");
  }
}
