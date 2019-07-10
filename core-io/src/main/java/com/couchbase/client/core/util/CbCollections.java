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

package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSet;

@Stability.Internal
public class CbCollections {
  private CbCollections() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Returns a new unmodifiable list with the same contents as the given collection.
   *
   * @param c may be {@code null}, in which case an empty list is returned.
   */
  public static <T> List<T> copyToUnmodifiableList(Collection<T> c) {
    return isNullOrEmpty(c) ? emptyList() : unmodifiableList(new ArrayList<>(c));
  }

  /**
   * Returns a new unmodifiable set with the same contents as the given collection.
   *
   * @param c may be {@code null}, in which case an empty set is returned.
   */
  public static <T> Set<T> copyToUnmodifiableSet(Collection<T> c) {
    return isNullOrEmpty(c) ? emptySet() : unmodifiableSet(new HashSet<>(c));
  }

  public static boolean isNullOrEmpty(Collection<?> c) {
    return c == null || c.isEmpty();
  }

  @SafeVarargs
  public static <T> Set<T> setOf(T... items) {
    return new HashSet<>(Arrays.asList(items));
  }
}
