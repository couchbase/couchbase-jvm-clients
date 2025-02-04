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

import java.util.List;
import java.util.Map;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.transform;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

@Stability.Volatile
public enum StorageSizeUnit {
  BYTES("B", 1L),
  KIBIBYTES("KiB", 1024L),
  MEBIBYTES("MiB", 1024L * 1024),
  GIBIBYTES("GiB", 1024L * 1024 * 1024),
  TEBIBYTES("TiB", 1024L * 1024 * 1024 * 1024),
  PEBIBYTES("PiB", 1024L * 1024 * 1024 * 1024 * 1024),
  ;

  final String abbreviation;
  final long bytesPerUnit;

  static final List<StorageSizeUnit> valueList = listOf(values());
  private static final List<String> abbreviations = transform(valueList, it -> it.abbreviation);

  private static final Map<String, StorageSizeUnit> abbreviationToUnit = unmodifiableMap(
    valueList.stream().collect(toMap(it -> it.abbreviation, it -> it))
  );

  StorageSizeUnit(String abbreviation, long bytesPerUnit) {
    this.abbreviation = requireNonNull(abbreviation);
    this.bytesPerUnit = bytesPerUnit;
  }

  static StorageSizeUnit parse(String abbreviation) {
    StorageSizeUnit result = abbreviationToUnit.get(abbreviation);
    if (result == null) {
      throw new IllegalArgumentException("Unrecognized storage unit '" + abbreviation + "' ;  expected one of " + StorageSizeUnit.abbreviations);
    }
    return result;
  }

  /**
   * Returns the largest unit that can exactly represent the given number of bytes.
   */
  static StorageSizeUnit largestExactUnit(long bytes) {
    if (bytes == 0) {
      return BYTES;
    }
    for (int i = valueList.size() - 1; i > 0; i--) {
      StorageSizeUnit unit = valueList.get(i);
      if (bytes % unit.bytesPerUnit == 0) {
        return unit;
      }
    }
    return BYTES;
  }
}
