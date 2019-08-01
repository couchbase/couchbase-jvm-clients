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

import java.util.Objects;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;

public class CollectionSpec {

  private final String name;
  private final String scopeName;

  private CollectionSpec(String name, String scopeName) {
    this.name = name;
    this.scopeName = scopeName;
  }

  public static CollectionSpec create(final String name, final String scopeName) {
    return new CollectionSpec(name, scopeName);
  }

  public String name() {
    return name;
  }

  public String scopeName() {
    return scopeName;
  }

  @Override
  public String toString() {
    return "CollectionSpec{" +
      "name='" + redactMeta(name) + '\'' +
      ", scopeName='" + redactMeta(scopeName) + '\'' +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CollectionSpec that = (CollectionSpec) o;
    return Objects.equals(name, that.name) &&
      Objects.equals(scopeName, that.scopeName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, scopeName);
  }
}
