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
import com.couchbase.client.core.kv.CoreRangeScan;
import com.couchbase.client.core.kv.CoreScanTerm;
import reactor.util.annotation.Nullable;

import java.util.Optional;

/**
 * Performs a KV range scan to scan between two {@link ScanTerm ScanTerms}.
 * <p>
 * Use {@link ScanType#rangeScan(ScanTerm, ScanTerm)} to construct.
 */
@Stability.Volatile
public class RangeScan extends ScanType {

  @Nullable private final ScanTerm from;
  @Nullable private final ScanTerm to;

  RangeScan(
    @Nullable final ScanTerm from,
    @Nullable final ScanTerm to
  ) {
    this.from = from;
    this.to = to;
  }

  /**
   * Returns the {@link ScanTerm} used to start scanning from.
   *
   * @return the {@link ScanTerm} used to start scanning from.
   */
  public Optional<ScanTerm> from() {
    return Optional.ofNullable(from);
  }

  /**
   * Returns the {@link ScanTerm} to scan to.
   *
   * @return the {@link ScanTerm} to scan to.
   */
  public Optional<ScanTerm> to() {
    return Optional.ofNullable(to);
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  @Stability.Internal
  public class Built implements CoreRangeScan {
    public CoreScanTerm from() {
      return from == null ? CoreScanTerm.MIN : from.toCore();
    }

    public CoreScanTerm to() {
      return to == null ? CoreScanTerm.MAX : to.toCore();
    }
  }
}
