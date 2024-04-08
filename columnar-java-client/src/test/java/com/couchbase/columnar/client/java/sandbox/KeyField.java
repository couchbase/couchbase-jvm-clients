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

package com.couchbase.columnar.client.java.sandbox;

import com.couchbase.client.core.annotation.Stability;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Field path and type.
 */
@Stability.Volatile
public class KeyField {

  /**
   * The subset of Columnar data types that can be used as a key,
   * plus the synthetic {@link #AUTO_GENERATED_UUID} type.
   */
  public enum Type {
    /**
     * A sequence of characters no longer than {@value Integer#MAX_VALUE} bytes.
     */
    STRING,

    /**
     * A 64-bit signed integer (known as a {@code long} in Java-land.)
     */
    BIGINT,

    /**
     * A 64-bit floating point number.
     *
     * @deprecated Floating point numbers make poor keys. Don't say we didn't warn you!
     */
    @Deprecated
    DOUBLE,

    /**
     * Automatically generated UUID.
     * <p>
     * Cannot be part of a composite key.
     */
    AUTO_GENERATED_UUID,
  }

  private final String path;
  private final Type type;

  /**
   * Specifies a field with the given path and type.
   *
   * @param path The path exactly as it should appear in a SQL statement;
   * caller is responsible for escaping, if necessary.
   */
  public KeyField(String path, Type type) {
    this.path = requireNonNull(path);
    this.type = requireNonNull(type);
  }

  public String path() {
    return path;
  }

  public Type type() {
    return type;
  }

  public String toSql() {
    return path + ": " + type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    KeyField that = (KeyField) o;
    return path.equals(that.path) && type == that.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, type);
  }

  @Override
  public String toString() {
    return toSql();
  }
}
