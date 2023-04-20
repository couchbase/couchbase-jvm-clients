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
import com.couchbase.client.core.kv.CoreScanTerm;

import static com.couchbase.client.core.util.CbStrings.MAX_CODE_POINT_AS_STRING;
import static com.couchbase.client.core.util.CbStrings.MIN_CODE_POINT_AS_STRING;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * A single {@link ScanTerm} identifying either the point to scan from or to scan to.
 */
@Stability.Volatile
public class ScanTerm {

  static private final ScanTerm MINIMUM = inclusive(MIN_CODE_POINT_AS_STRING);
  static private final ScanTerm MAXIMUM = inclusive(MAX_CODE_POINT_AS_STRING);

  /**
   * Contains the key pattern of this term.
   */
  private final String id;

  /**
   * If the pattern in the scan is considered exclusive or inclusive.
   */
  private final boolean exclusive;

  /**
   * Creates a new {@link ScanTerm}.
   *
   * @param id the key pattern of this term.
   * @param exclusive if the term is exclusive while scanning.
   */
  private ScanTerm(final String id, final boolean exclusive) {
    this.id = notNullOrEmpty(id, "ScanTerm ID");
    this.exclusive = exclusive;
  }

  /**
   * Returns the key pattern of this term.
   */
  public String id() {
    return id;
  }

  public boolean exclusive() {
    return exclusive;
  }

  /**
   * Creates an inclusive {@link ScanTerm} from a UTF-8 string.
   *
   * @param id the document ID / pattern to use as the scan term.
   * @return the created {@link ScanTerm}.
   */
  public static ScanTerm inclusive(final String id) {
    return new ScanTerm(id, false);
  }

  /**
   * Creates an exclusive {@link ScanTerm} from a UTF-8 string.
   *
   * @param id the document ID / pattern to use as the scan term.
   * @return the created {@link ScanTerm}.
   */
  public static ScanTerm exclusive(final String id) {
    return new ScanTerm(id, true);
  }

  /**
   * Returns a scan term representing the absolute minimum pattern (starting point).
   * <p>
   * Equivalent to {@code ScanTerm.inclusive(Character.toString(Character.MIN_CODE_POINT))}
   *
   * @return the absolute minimum {@link ScanTerm}.
   */
  public static ScanTerm minimum() {
    return MINIMUM;
  }

  /**
   * Returns a scan term representing the absolute maximum pattern (end point).
   * <p>
   * Equivalent to {@code ScanTerm.inclusive(Character.toString(Character.MAX_CODE_POINT))}
   *
   * @return the absolute maximum {@link ScanTerm}.
   */
  public static ScanTerm maximum() {
    return MAXIMUM;
  }

  @Stability.Internal
  public CoreScanTerm toCore() {
    return new CoreScanTerm(id, exclusive);
  }

}
