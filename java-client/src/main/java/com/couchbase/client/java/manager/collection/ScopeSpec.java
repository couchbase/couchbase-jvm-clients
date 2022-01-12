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

import com.couchbase.client.core.annotation.Stability;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;

/**
 * The {@link ScopeSpec} describes properties of a scope that can be managed.
 */
public class ScopeSpec {

  /**
   * The name of the scope.
   */
  private final String name;

  /**
   * The underlying {@link CollectionSpec}s.
   */
  private final Set<CollectionSpec> collectionSpecs;

  /**
   * Creates a new {@link ScopeSpec} with a set of {@link CollectionSpec}s.
   *
   * @param name the name of the scope.
   * @param collectionSpecs the collections that should be part of the scope.
   */
  private ScopeSpec(final String name, final Set<CollectionSpec> collectionSpecs) {
    this.name = name;
    this.collectionSpecs = collectionSpecs;
  }

  /**
   * Creates a new {@link ScopeSpec} with a set of {@link CollectionSpec}s.
   *
   * @param name the name of the scope.
   * @param collectionSpecs the collections that should be part of the scope.
   * @return the created {@link ScopeSpec}.
   */
  @Stability.Internal
  public static ScopeSpec create(final String name, final Set<CollectionSpec> collectionSpecs) {
    return new ScopeSpec(name, collectionSpecs);
  }

  /**
   * Creates a new {@link ScopeSpec} with no collections attached.
   *
   * @param name the name of the scope.
   * @return the created {@link ScopeSpec}.
   */
  @Stability.Internal
  public static ScopeSpec create(final String name) {
    return create(name, Collections.emptySet());
  }

  /**
   * Returns the name of the scope.
   *
   * @return the name of the scope.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the underlying collections of this scope.
   *
   * @return the underlying collections of this scope.
   */
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
