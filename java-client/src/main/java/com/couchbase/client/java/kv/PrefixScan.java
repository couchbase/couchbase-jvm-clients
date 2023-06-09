/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.kv.CoreRangeScan;
import com.couchbase.client.core.kv.CoreScanType;

import static java.util.Objects.requireNonNull;

/**
 * Performs a KV range scan for documents whose IDs start with a certain prefix.
 * <p>
 * Use {@link ScanType#prefixScan(String)} to construct.
 */
@Stability.Volatile
public class PrefixScan extends ScanType {
  private final String prefix;

  PrefixScan(String prefix) {
    this.prefix = requireNonNull(prefix);
  }

  public String prefix() {
    return prefix;
  }

  @Override
  public CoreScanType build() {
    return CoreRangeScan.forPrefix(prefix);
  }

  @Override
  public String toString() {
    return "PrefixScan{" +
      "prefix='" + prefix + '\'' +
      '}';
  }
}
