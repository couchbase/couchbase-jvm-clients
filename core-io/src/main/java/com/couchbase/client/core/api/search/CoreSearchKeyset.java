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

package com.couchbase.client.core.api.search;

import com.couchbase.client.core.annotation.Stability;

import java.util.List;

import static com.couchbase.client.core.util.CbCollections.copyToUnmodifiableList;
import static java.util.Collections.emptyList;

@Stability.Internal
public class CoreSearchKeyset {
  private final List<String> keys;

  public static final CoreSearchKeyset EMPTY = new CoreSearchKeyset(emptyList());

  public CoreSearchKeyset(List<String> keys) {
    this.keys = copyToUnmodifiableList(keys);
  }

  public List<String> keys() {
    return keys;
  }

  @Override
  public String toString() {
    return keys.toString();
  }
}
