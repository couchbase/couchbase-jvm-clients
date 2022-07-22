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
import com.couchbase.client.core.error.InvalidArgumentException;

import java.util.Optional;

/**
 * Performs a KV range scan using random sampling.
 * <p>
 * Use {@link ScanType#samplingScan(long)} and {@link ScanType#samplingScan(long, long)} to construct.
 */
@Stability.Volatile
public class SamplingScan extends ScanType {
  private final long limit;
  private final Optional<Long> seed;

  SamplingScan(final long limit, final Optional<Long> seed) {
    if (limit <= 0) {
      throw InvalidArgumentException.fromMessage("The limit of the SamplingScan must be greater than 0");
    }
    this.limit = limit;
    this.seed = seed;
  }

  /**
   * Returns the limit set for this sampling scan.
   *
   * @return the limit set for this sampling scan.
   */
  public long limit() {
    return limit;
  }

  /**
   * Returns the seed set for this sampling scan.
   *
   * @return the seed set for this sampling scan.
   */
  public Optional<Long> seed() {
    return seed;
  }

}
