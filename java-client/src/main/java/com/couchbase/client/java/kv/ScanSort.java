/*
 * Copyright (c) 2022 Couchbase, Inc.
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

package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.kv.CoreRangeScanSort;
import com.couchbase.client.core.kv.CoreScanTerm;

/**
 * The sorting used for the scan operation.
 */
@Stability.Volatile
public enum ScanSort {

  /**
   * Do not sort the results.
   * <p>
   * Fast and efficient. Results are emitted in the order they arrive from each partition.
   */
  NONE,

  /**
   * Sort the results by document ID in lexicographic order.
   * <p>
   * Reasonably efficient. Suitable for large range scans, since it *does not*
   * require buffering the entire result set in memory.
   * <p>
   * <b>CAVEAT:</b> When used with a {@link ScanType#samplingScan} scan, the behavior is unspecified
   * and may change in a future version. Likely outcomes include:
   * <ul>
   *   <li> Skewing the results towards "low" document IDs.
   *   <li> Not actually sorting.
   * </ul>
   * If you require sorted results from a sample scan, please use {@link ScanSort#NONE},
   * then collect the results into a list and sort the list.
   */
  ASCENDING;

  @Stability.Internal
  public CoreRangeScanSort intoCore() {
    if (this == NONE) {
      return CoreRangeScanSort.NONE;
    } else if (this == ASCENDING) {
      return CoreRangeScanSort.ASCENDING;
    } else {
      throw new IllegalStateException("Unsupported ScanSort Type");
    }
  }

}
