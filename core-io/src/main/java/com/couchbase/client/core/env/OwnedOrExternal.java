/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.client.core.env;

import com.couchbase.client.core.annotation.Stability;

import static java.util.Objects.requireNonNull;

/**
 * A holder for a resource that is either "owned" (created and managed
 * by the SDK) or "external" (created and managed outside the SDK).
 * <p>
 * <b>Note:</b> Although this class bears a passing resemblance to
 * {@link java.util.function.Supplier}, it has different semantics.
 * Unlike {@code Supplier.get()}, this class's {@link #get()}
 * method is guaranteed to return the same instance every time.
 */
@Stability.Internal
public class OwnedOrExternal<T> {
  private final boolean owned;
  private final T resource;

  public static <T> OwnedOrExternal<T> owned(T resource) {
    return new OwnedOrExternal<>(true, resource);
  }

  public static <T> OwnedOrExternal<T> external(T resource) {
    return new OwnedOrExternal<>(false, resource);
  }

  private OwnedOrExternal(boolean owned, T resource) {
    this.owned = owned;
    this.resource = requireNonNull(resource);
  }

  /**
   * Returns true if the resource is managed by the SDK, otherwise false.
   */
  public boolean isOwned() {
    return owned;
  }

  /**
   * Returns the resource.
   */
  public T get() {
    return resource;
  }

  @Override
  public String toString() {
    return (owned ? "Owned" : "External") + "{" + resource + "}";
  }
}
