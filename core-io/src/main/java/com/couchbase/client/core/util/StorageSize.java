/*
 * Copyright 2025 Couchbase, Inc.
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

package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonValue;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * A size measured in bytes, kibibytes, mebibytes, etc.
 * <p>
 * Create an instance using one of the "of" factory methods,
 * or from a string using {@link #parse(String)}.
 *
 * @see #ofBytes
 * @see #ofKibibytes
 * @see #ofMebibytes
 * @see #ofGibibytes
 * @see #ofTebibytes
 * @see #ofPebibytes
 */
@Stability.Volatile
public final class StorageSize {
  private static final Pattern PATTERN = Pattern.compile("(\\d+)\\s*(.+)");

  private final long bytes;
  private final String formatted;

  private StorageSize(long bytes, String formatted) {
    if (bytes < 0) {
      throw new IllegalArgumentException("Storage size cannot be negative");
    }

    this.bytes = bytes;
    this.formatted = requireNonNull(formatted);
  }

  /**
   * Creates a new instance from its string representation:
   * a whole number followed by a unit, optionally separated by whitespace.
   * Recognized units: B, KiB, MiB, GiB, TiB, PiB.
   * <p>
   * Examples:
   * <ul>
   *   <li>"128B" or "128 B" -> 128 bytes
   *   <li>"512KiB" or "512 KiB" -> 512 kibibytes
   *   <li>"64MiB" or "64 MiB" -> 64 mebibytes
   *   <li>etc.
   * </ul>
   *
   * @throws IllegalArgumentException if the given string could not be parsed
   */
  public static StorageSize parse(String s) {
    final Matcher m = PATTERN.matcher(s.trim());
    try {
      if (!m.matches()) {
        throw new IllegalArgumentException("Expected a whole number followed by a supported size unit (B, KiB, MiB, etc.)");
      }

      long value = Long.parseLong(m.group(1));
      StorageSizeUnit unit = StorageSizeUnit.parse(m.group(2));
      return StorageSize.of(value, unit);

    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unable to parse storage size '" + s + "' ; " + e.getMessage(), e);
    }
  }

  /**
   * @throws IllegalArgumentException if value is negative
   */
  public static StorageSize ofBytes(long value) {
    return StorageSize.of(value, StorageSizeUnit.BYTES);
  }

  /**
   * @throws IllegalArgumentException if value is negative, or the size is greater than {@link Long#MAX_VALUE} bytes (~8191 PiB)
   */
  public static StorageSize ofKibibytes(long value) {
    return StorageSize.of(value, StorageSizeUnit.KIBIBYTES);
  }

  /**
   * @throws IllegalArgumentException if value is negative, or the size is greater than {@link Long#MAX_VALUE} bytes (~8191 PiB)
   */
  public static StorageSize ofMebibytes(long value) {
    return StorageSize.of(value, StorageSizeUnit.MEBIBYTES);
  }

  /**
   * @throws IllegalArgumentException if value is negative, or the size is greater than {@link Long#MAX_VALUE} bytes (~8191 PiB)
   */
  public static StorageSize ofGibibytes(long value) {
    return StorageSize.of(value, StorageSizeUnit.GIBIBYTES);
  }

  /**
   * @throws IllegalArgumentException if value is negative, or the size is greater than {@link Long#MAX_VALUE} bytes (~8191 PiB)
   */
  public static StorageSize ofTebibytes(long value) {
    return StorageSize.of(value, StorageSizeUnit.TEBIBYTES);
  }

  /**
   * @throws IllegalArgumentException if value is negative, or the size is greater than {@link Long#MAX_VALUE} bytes (~8191 PiB)
   */
  public static StorageSize ofPebibytes(long value) {
    return StorageSize.of(value, StorageSizeUnit.PEBIBYTES);
  }

  /**
   * @throws IllegalArgumentException if value is negative, or the size is greater than {@link Long#MAX_VALUE} bytes (~8191 PiB)
   */
  static StorageSize of(long value, StorageSizeUnit unit) {
    final long bytes;
    try {
      bytes = Math.multiplyExact(value, unit.bytesPerUnit);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(value + " " + unit.abbreviation + " is larger than the limit of " + Long.MAX_VALUE + " bytes.");
    }

    StorageSizeUnit simplifiedUnit = StorageSizeUnit.largestExactUnit(bytes);
    long simplifiedValue = bytes / simplifiedUnit.bytesPerUnit;
    return new StorageSize(bytes, simplifiedValue + " " + simplifiedUnit.abbreviation);
  }

  /**
   * Returns the storage size in bytes.
   */
  public long bytes() {
    return bytes;
  }

  /**
   * Returns the storage size in bytes.
   *
   * @throws IllegalArgumentException if this size is >= 2 GiB
   */
  public int bytesAsInt() {
    return (int) requireInt().bytes;
  }

  @Stability.Internal
  public StorageSize requireInt() {
    if (bytes > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("This size must be less than 2 GiB, but got: " + format());
    }
    return this;
  }

  @JsonValue
  public String format() {
    return formatted;
  }

  @Override
  public String toString() {
    return format();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    StorageSize that = (StorageSize) o;
    return bytes == that.bytes;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(bytes);
  }
}
