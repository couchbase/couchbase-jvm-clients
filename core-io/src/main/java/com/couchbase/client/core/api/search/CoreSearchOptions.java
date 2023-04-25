/*
 * Copyright (c) 2023 Couchbase, Inc.
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

package com.couchbase.client.core.api.search;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.facet.CoreSearchFacet;
import com.couchbase.client.core.api.search.sort.CoreSearchSort;
import com.couchbase.client.core.api.shared.CoreMutationState;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.error.InvalidArgumentException;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.Map;

@Stability.Internal
public
interface CoreSearchOptions {

  List<String> collections();

  @Nullable
  CoreSearchScanConsistency consistency();

  @Nullable
  CoreMutationState consistentWith();

  @Nullable
  Boolean disableScoring();

  @Nullable
  Boolean explain();

  Map<String, CoreSearchFacet> facets();

  List<String> fields();

  List<String> highlightFields();

  @Nullable
  CoreHighlightStyle highlightStyle();

  @Nullable
  Integer limit();

  @Nullable
  JsonNode raw();

  @Nullable
  Integer skip();

  @Nullable
  default CoreSearchKeyset searchBefore() {
    return null;
  }

  @Nullable
  default CoreSearchKeyset searchAfter() {
    return null;
  }

  List<CoreSearchSort> sort();

  @Nullable
  Boolean includeLocations();

  CoreCommonOptions commonOptions();

  default void validate() {
    int pageOptions = 0;
    if (skip() != null) {
      pageOptions++;
    }
    if (searchBefore() != null) {
      pageOptions++;
    }
    if (searchAfter() != null) {
      pageOptions++;
    }
    if (pageOptions > 1) {
      throw InvalidArgumentException.fromMessage(
          "Must specify no more than one of 'skip', 'searchBefore', or 'searchAfter'."
      );
    }
  }

}
