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

package com.couchbase.client.core.api.search.facet;

import com.couchbase.client.core.annotation.Stability;
import reactor.util.annotation.Nullable;

@Stability.Internal
public class CoreNumericRange {
  private final String name;
  private final @Nullable Double min;
  private final @Nullable Double max;

  public CoreNumericRange(String name, @Nullable Double min, @Nullable Double max) {
    this.name = name;
    this.min = min;
    this.max = max;
  }

  public String name() {
    return name;
  }

  public @Nullable Double min() {
    return min;
  }

  public @Nullable Double max() {
    return max;
  }
}
