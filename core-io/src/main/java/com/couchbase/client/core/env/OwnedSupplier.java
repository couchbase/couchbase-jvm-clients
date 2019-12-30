/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.env;

import com.couchbase.client.core.annotation.Stability;

import java.util.function.Supplier;

/**
 * A special supplier which allows the SDK to distinguish passed in suppliers vs. owned ones.
 * <p>
 * <strong>DO NOT USE THIS CLASS DIRECTLY!</strong>
 */
@Stability.Internal
public class OwnedSupplier<T> implements Supplier<T> {

  private final T value;

  @Stability.Internal
  public OwnedSupplier(final T value) {
    this.value = value;
  }

  @Override
  public T get() {
    return value;
  }

  @Override
  public String toString() {
    return "OwnedSupplier{" + value + '}';
  }

}
