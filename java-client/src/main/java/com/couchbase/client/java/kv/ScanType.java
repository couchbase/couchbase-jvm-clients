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
import com.couchbase.client.core.kv.CoreScanType;

import java.util.Optional;

import static com.couchbase.client.core.util.CbStrings.MAX_CODE_POINT_AS_STRING;

/**
 * Specifies which documents to include in a KV scan.
 * <p>
 * Create an instance using one of the static factory methods:
 * <ul>
 *   <li>{@link #rangeScan()} - All documents.</li>
 *   <li>{@link #rangeScan(ScanTerm, ScanTerm)} - All documents whose IDs are in a certain range.</li>
 *   <li>{@link #prefixScan(String)} - All documents whose IDs have a certain prefix.</li>
 *   <li>{@link #samplingScan(long)} - A random sample of documents.</li>
 * </ul>
 */
@Stability.Volatile
public abstract class ScanType {

  /**
   * Specifies a range scan that includes all documents in the collection.
   *
   * @return a newly created {@link RangeScan} to be passed into the Collection API.
   */
  public static RangeScan rangeScan() {
    return rangeScan(ScanTerm.minimum(), ScanTerm.maximum());
  }

  /**
   * Specifies a range scan that includes all documents whose IDs are between two {@link ScanTerm ScanTerms}.
   *
   * @param from the start of the range
   * @param to the end of the range
   * @return a newly created {@link RangeScan} to be passed into the Collection API.
   */
  public static RangeScan rangeScan(final ScanTerm from, final ScanTerm to) {
    return new RangeScan(from, to);
  }

  /**
   * Specifies a range scan that includes all documents whose IDs start with the given prefix.
   *
   * @return a newly created {@link RangeScan} to be passed into the Collection API.
   */
  public static RangeScan prefixScan(final String documentIdPrefix) {
    return new RangeScan(
      ScanTerm.inclusive(documentIdPrefix),
      ScanTerm.exclusive(documentIdPrefix + MAX_CODE_POINT_AS_STRING)
    );
  }

  /**
   * Creates a new KV sampling scan, which randomly selects documents up until the configured limit, with a random seed.
   *
   * @param limit the number of documents to limit sampling to.
   * @return a newly created {@link RangeScan} to be passed into the Collection API.
   */
  public static SamplingScan samplingScan(final long limit) {
    return new SamplingScan(limit, Optional.empty());
  }

  /**
   * Creates a new KV sampling scan, which randomly selects documents up until the configured limit, with the specified seed.
   *
   * @param limit the number of documents to limit sampling to.
   * @param seed seed for the random number generator that selects the documents.
   * <b>CAVEAT</b>: Specifying the same seed does not guarantee the same documents are selected.
   * @return a newly created {@link RangeScan} to be passed into the Collection API.
   */
  public static SamplingScan samplingScan(final long limit, final long seed) {
    return new SamplingScan(limit, Optional.of(seed));
  }

  public abstract CoreScanType build();

}
