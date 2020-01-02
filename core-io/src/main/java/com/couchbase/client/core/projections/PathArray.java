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

package com.couchbase.client.core.projections;

import java.util.Objects;

public class PathArray implements PathElement {
  private final String str;
  private final int idx;

  public PathArray(String str, int idx) {
    this.str = str;
    this.idx = idx;
  }

  public String str() {
    return str;
  }

  public int idx() {
    return idx;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PathArray pathArray = (PathArray) o;
    return idx == pathArray.idx &&
            Objects.equals(str, pathArray.str);
  }

  @Override
  public int hashCode() {
    return Objects.hash(str, idx);
  }

  @Override
  public String toString() {
    return "PathArray{" + str + '[' + idx + "]}";
  }
}
