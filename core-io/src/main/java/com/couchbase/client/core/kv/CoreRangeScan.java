/*
 * Copyright (c) 2023 Couchbase, Inc.
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

package com.couchbase.client.core.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.util.CbStrings;

/**
 * Performs a KV range scan to scan between two {@link CoreScanTerm CoreScanTerms}.
 */
@Stability.Internal
public interface CoreRangeScan extends CoreScanType {

  CoreScanTerm from();

  CoreScanTerm to();

  static CoreRangeScan forPrefix(String prefix) {
    return new CoreRangeScan() {
      @Override
      public CoreScanTerm from() {
        return new CoreScanTerm(prefix, false);
      }

      @Override
      public CoreScanTerm to() {
        return new CoreScanTerm(prefix + CbStrings.MAX_CODE_POINT_AS_STRING, true);
      }
    };
  }

}
