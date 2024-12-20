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
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CbObjects {
  private CbObjects() {
    throw new AssertionError("not instantiable");
  }

  /**
   * @implNote NullUnmarked because the nullability of the return value depends on the nullability of {@code defaultValue}.
   * At some point we could revisit this, perhaps by adding a nullableDefaultIfNull() flavor that is explicitly nullable,
   * and making defaultIfNull reject null default values.
   */
  @NullUnmarked
  public static <T> T defaultIfNull(T value, T defaultValue) {
    return value == null ? defaultValue : value;
  }

  /**
   * @implNote NullUnmarked because the nullability of the return value depends on the nullability of {@code defaultValue}.
   * At some point we could revisit this, perhaps by adding a nullableDefaultIfNull() flavor that is explicitly nullable,
   * and making defaultIfNull reject null default values.
   */
  @NullUnmarked
  public static <T> T defaultIfNull(T value, Supplier<? extends @Nullable T> defaultValueSupplier) {
    requireNonNull(defaultValueSupplier);
    return value == null ? defaultValueSupplier.get() : value;
  }
}
