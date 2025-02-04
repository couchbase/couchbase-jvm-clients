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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StorageSizeTest {
  private static void assertIllegalArg(Executable executable) {
    assertThrows(IllegalArgumentException.class, executable);
  }

  private static void assertParseFailure(String formatted) {
    assertIllegalArg(() -> StorageSize.parse(formatted));
  }

  @Test
  void parseThrowsIfMissingOrUnrecognizedUnit() {
    assertParseFailure("128");
    assertParseFailure("128FiB");
    assertParseFailure("-128FiB");
  }

  @Test
  void unitsAreCaseSensitive() {
    assertParseFailure("128b");
    assertParseFailure("128kib");
    assertParseFailure("128mib");
    assertParseFailure("128gib");
    assertParseFailure("128tib");
    assertParseFailure("128pib");
  }

  @Test
  void rejectsSiAndAmbiguousUnits() {
    assertParseFailure("128k");
    assertParseFailure("128K");
    assertParseFailure("128KB");

    assertParseFailure("128m");
    assertParseFailure("128M");
    assertParseFailure("128MB");
  }

  @Test
  void canParseAllUnits() {
    assertEquals(StorageSize.ofBytes(2), StorageSize.parse("2 B"));
    assertEquals(StorageSize.ofKibibytes(10), StorageSize.parse("10 KiB"));
    assertEquals(StorageSize.ofMebibytes(2), StorageSize.parse("2 MiB"));
    assertEquals(StorageSize.ofGibibytes(10), StorageSize.parse("10 GiB"));
    assertEquals(StorageSize.ofTebibytes(128), StorageSize.parse("128 TiB"));
    assertEquals(StorageSize.ofPebibytes(128), StorageSize.parse("128 PiB"));
  }

  @Test
  void whitespaceIsOptional() {
    assertEquals(StorageSize.ofKibibytes(10), StorageSize.parse("10KiB"));
    assertEquals(StorageSize.ofKibibytes(10), StorageSize.parse("10 KiB"));
  }

  @Test
  void toleratesAnyKindOrAmountOfWhitespace() {
    assertEquals(StorageSize.ofKibibytes(10), StorageSize.parse("10  KiB"));
    assertEquals(StorageSize.ofKibibytes(10), StorageSize.parse("10\tKiB"));
    assertEquals(StorageSize.ofKibibytes(10), StorageSize.parse("10\nKiB"));
  }

  @Test
  void toleratesLeadingAndTailingWhitespace() {
    assertEquals(StorageSize.ofKibibytes(10), StorageSize.parse(" 10 KiB "));
  }

  @Test
  void equalsMeansSameBytes() {
    assertEquals(StorageSize.ofKibibytes(10), StorageSize.ofBytes(10 * 1024));
    assertNotEquals(StorageSize.ofKibibytes(10), StorageSize.ofBytes(1024));
  }

  @Test
  void simplifiesFormatString() {
    assertEquals("1 GiB", StorageSize.ofKibibytes(1024 * 1024).format());
    assertEquals("0 B", StorageSize.ofPebibytes(0).format());
  }

  @Test
  void onlySimplifiesWholeUnit() {
    assertEquals(
      "1536 KiB", // not "1.5 MiB", for example
      StorageSize.ofKibibytes(1024 + 512).format()
    );
  }

  @Test
  void of() {
    assertEquals(2, StorageSize.ofBytes(2).bytes());
    assertEquals(2_048, StorageSize.ofKibibytes(2).bytes());
    assertEquals(2_097_152, StorageSize.ofMebibytes(2).bytes());
    assertEquals(2_147_483_648L, StorageSize.ofGibibytes(2).bytes());
    assertEquals(2_199_023_255_552L, StorageSize.ofTebibytes(2).bytes());
    assertEquals(2_251_799_813_685_248L, StorageSize.ofPebibytes(2).bytes());

    assertEquals(2, StorageSize.of(2, StorageSizeUnit.BYTES).bytes());
    assertEquals(2_048, StorageSize.of(2, StorageSizeUnit.KIBIBYTES).bytes());
    assertEquals(2_097_152, StorageSize.of(2, StorageSizeUnit.MEBIBYTES).bytes());
    assertEquals(2_147_483_648L, StorageSize.of(2, StorageSizeUnit.GIBIBYTES).bytes());
    assertEquals(2_199_023_255_552L, StorageSize.of(2, StorageSizeUnit.TEBIBYTES).bytes());
    assertEquals(2_251_799_813_685_248L, StorageSize.of(2, StorageSizeUnit.PEBIBYTES).bytes());
  }

  @Test
  void negativeIsIllegalArgument() {
    assertIllegalArg(() -> StorageSize.ofKibibytes(-1));
    assertIllegalArg(() -> StorageSize.of(-1, StorageSizeUnit.KIBIBYTES));
  }

  @Test
  void requireIntThrowsIllegalArgumentIfOutOfRange() {
    assertIllegalArg(() -> StorageSize.ofKibibytes(1L + Integer.MAX_VALUE).requireInt());
    assertIllegalArg(() -> StorageSize.ofKibibytes(Long.MAX_VALUE).requireInt());

    assertIllegalArg(() -> StorageSize.ofKibibytes(1L + Integer.MAX_VALUE).bytesAsInt());
    assertIllegalArg(() -> StorageSize.ofKibibytes(Long.MAX_VALUE).bytesAsInt());
  }

  @Test
  void canGetBytesAsIntIfInRange() {
    assertEquals(0, StorageSize.ofBytes(0).bytesAsInt());
    assertEquals(Integer.MAX_VALUE, StorageSize.ofBytes(Integer.MAX_VALUE).bytesAsInt());
  }

  @Test
  void canRoundTrip() {
    for (StorageSizeUnit unit : StorageSizeUnit.values()) {
      StorageSize original = StorageSize.of(128, unit);
      StorageSize parsed = StorageSize.parse(original.format());
      assertEquals(original, parsed);
      assertEquals(original.format(), parsed.format());
    }
  }
}
