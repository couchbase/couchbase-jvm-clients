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

/**
 * The sorting used for the scan operation.
 */
@Stability.Volatile
public enum ScanSort {

  /**
   * No sorting is applied, the results are returned as they are streamed from the servers.
   */
  NONE,
  /**
   * Ascending sorting is applied to all the keys in the results.
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
