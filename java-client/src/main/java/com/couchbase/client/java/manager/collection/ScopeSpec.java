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

package com.couchbase.client.java.manager.collection;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;

public class ScopeSpec {

  private final String name;

  private final Set<CollectionSpec> collectionSpecs;

  private ScopeSpec(final String name, final Set<CollectionSpec> collectionSpecs) {
    this.name = name;
    this.collectionSpecs = collectionSpecs;
  }

  public static ScopeSpec create(final String name, final Set<CollectionSpec> collectionSpecs) {
    return new ScopeSpec(name, collectionSpecs);
  }

  public static ScopeSpec create(final String name) {
    return create(name, Collections.emptySet());
  }

  public String name() {
    return name;
  }

  public Set<CollectionSpec> collections() {
    return collectionSpecs;
  }

  @Override
  public String toString() {
    return "ScopeSpec{" +
      "name='" + redactMeta(name) +
      ", collectionSpecs=" + collectionSpecs +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ScopeSpec scopeSpec = (ScopeSpec) o;
    return Objects.equals(name, scopeSpec.name) &&
      Objects.equals(collectionSpecs, scopeSpec.collectionSpecs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, collectionSpecs);
  }
}
