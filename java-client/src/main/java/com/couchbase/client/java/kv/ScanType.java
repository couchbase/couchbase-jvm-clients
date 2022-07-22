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

import java.util.Optional;

/**
 * Base class for the different range scan types available.
 * <p>
 * Right now there are two different types available:
 * <ul>
 *   <li>Range Scan: see {@link #rangeScan(ScanTerm, ScanTerm)}.</li>
 *   <li>Sampling Scan: see {@link #samplingScan(long)} and {@link #samplingScan(long, long)}.</li>
 * </ul>
 */
@Stability.Volatile
public abstract class ScanType {

  /**
   * Creates a new KV range scan, scanning between two {@link ScanTerm ScanTerms}.
   *
   * @param from the {@link ScanTerm} to start scanning from.
   * @param to the {@link ScanTerm} to scan to.
   * @return a newly created {@link RangeScan} to be passed into the Collection API.
   */
  public static RangeScan rangeScan(final ScanTerm from, final ScanTerm to) {
    return new RangeScan(from, to);
  }

  /**
   * Creates a new KV sampling scan, which randomly samples documents up until the configured limit and no custom seed.
   *
   * @param limit the number of documents to limit sampling to.
   * @return a newly created {@link RangeScan} to be passed into the Collection API.
   */
  public static SamplingScan samplingScan(final long limit) {
    return new SamplingScan(limit, Optional.empty());
  }

  /**
   * Creates a new KV sampling scan, which randomly samples documents up until the configured limit with a custom seed.
   *
   * @param limit the number of documents to limit sampling to.
   * @param seed the custom seed used for sampling.
   * @return a newly created {@link RangeScan} to be passed into the Collection API.
   */
  public static SamplingScan samplingScan(final long limit, final long seed) {
    return new SamplingScan(limit, Optional.of(seed));
  }

}
