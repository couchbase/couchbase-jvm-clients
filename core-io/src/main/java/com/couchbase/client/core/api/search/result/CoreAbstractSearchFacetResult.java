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
package com.couchbase.client.core.api.search.result;

import com.couchbase.client.core.annotation.Stability;

import static com.couchbase.client.core.util.Validators.notNull;

@Stability.Internal
public abstract class CoreAbstractSearchFacetResult implements CoreSearchFacetResult {

  protected final String name;
  protected final String field;
  protected final long total;
  protected final long missing;
  protected final long other;

  protected CoreAbstractSearchFacetResult(String facetName, String field, long total, long missing, long other) {
    this.name = notNull(facetName, "Facet Name");
    this.field = notNull(field, "Field");
    this.total = total;
    this.missing = missing;
    this.other = other;
  }

  @Override
  public String name() {
    return this.name;
  }

  @Override
  public String field() {
    return this.field;
  }

  @Override
  public long missing() {
    return this.missing;
  }

  @Override
  public long other() {
    return this.other;
  }

  @Override
  public long total() {
    return this.total;
  }
}
