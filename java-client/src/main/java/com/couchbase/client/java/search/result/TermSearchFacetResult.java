/*
 * Copyright (c) 2016 Couchbase, Inc.
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
package com.couchbase.client.java.search.result;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.result.CoreTermSearchFacetResult;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation of a {@link TermSearchFacetResult}.
 *
 * @since 2.3.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TermSearchFacetResult extends AbstractSearchFacetResult {

  private final CoreTermSearchFacetResult internal;

  @Stability.Internal
  public TermSearchFacetResult(CoreTermSearchFacetResult internal) {
    super(internal);
    this.internal = internal;
  }

  public List<SearchTermRange> terms() {
    return internal.terms()
            .stream()
            .map(SearchTermRange::new)
            .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return internal.toString();
  }
}
