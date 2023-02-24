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

package com.couchbase.client.java.search.util;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.result.CoreDateRangeSearchFacetResult;
import com.couchbase.client.core.api.search.result.CoreNumericRangeSearchFacetResult;
import com.couchbase.client.core.api.search.result.CoreSearchFacetResult;
import com.couchbase.client.core.api.search.result.CoreTermSearchFacetResult;
import com.couchbase.client.java.search.result.DateRangeSearchFacetResult;
import com.couchbase.client.java.search.result.NumericRangeSearchFacetResult;
import com.couchbase.client.java.search.result.SearchFacetResult;
import com.couchbase.client.java.search.result.TermSearchFacetResult;

@Stability.Internal
public class SearchFacetUtil {
  public static SearchFacetResult convert(CoreSearchFacetResult facet) {
    if (facet instanceof CoreDateRangeSearchFacetResult) {
      return new DateRangeSearchFacetResult((CoreDateRangeSearchFacetResult) facet);
    } else if (facet instanceof CoreNumericRangeSearchFacetResult) {
      return new NumericRangeSearchFacetResult((CoreNumericRangeSearchFacetResult) facet);
    } else if (facet instanceof CoreTermSearchFacetResult) {
      return new TermSearchFacetResult((CoreTermSearchFacetResult) facet);
    } else {
      throw new RuntimeException("Likely being used with an incompatible core-io - unknown SearchFacetResult");
    }
  }
}
