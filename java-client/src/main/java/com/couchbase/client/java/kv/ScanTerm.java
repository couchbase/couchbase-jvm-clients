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
import com.couchbase.client.core.kv.CoreScanOptions;
import com.couchbase.client.core.kv.CoreScanTerm;
import com.couchbase.client.java.CommonOptions;

import java.nio.charset.StandardCharsets;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * A single {@link ScanTerm} identifying either the point to scan from or to scan to.
 */
@Stability.Volatile
public class ScanTerm {

  private static final ScanTerm MINIMUM = inclusive(new byte[]{(byte) 0x00});
  private static final ScanTerm MAXIMUM = inclusive(new byte[]{(byte) 0xFF});

  /**
   * Contains the key pattern of this term.
   */
  private final byte[] id;

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
  private ScanTerm(final byte[] id, final boolean exclusive) {
    this.id = notNullOrEmpty(id, "ScanTerm ID")
        .clone();
    this.exclusive = exclusive;
  }

  private ScanTerm(final String id, final boolean exclusive) {
    this.id = notNullOrEmpty(id, "ScanTerm ID").getBytes(StandardCharsets.UTF_8);
    this.exclusive = exclusive;
  }

  /**
   * Returns a new byte array containing the key pattern of this term.
   */
  public byte[] id() {
    return id.clone();
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
   * Creates an inclusive {@link ScanTerm} from a byte array.
   *
   * @param id the document ID / pattern to use as the scan term.
   * @return the created {@link ScanTerm}.
   */
  public static ScanTerm inclusive(final byte[] id) {
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
   * Creates an exclusive {@link ScanTerm} from a byte array.
   *
   * @param id the document ID / pattern to use as the scan term.
   * @return the created {@link ScanTerm}.
   */
  public static ScanTerm exclusive(final byte[] id) {
    return new ScanTerm(id, true);
  }

  /**
   * Returns a scan term representing the absolute minimum pattern (starting point).
   * <p>
   * Equivalent to {@code ScanTerm.inclusive(new byte[]{(byte) 0x00})}
   *
   * @return the absolute minimum {@link ScanTerm}.
   */
  public static ScanTerm minimum() {
    return MINIMUM;
  }

  /**
   * Returns a scan term representing the absolute maximum pattern (end point).
   * <p>
   * Equivalent to {@code ScanTerm.inclusive(new byte[]{(byte) 0xFF})}
   *
   * @return the absolute maximum {@link ScanTerm}.
   */
  public static ScanTerm maximum() {
    return MAXIMUM;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  @Stability.Internal
  public class Built  implements CoreScanTerm {
    
    public byte[] id(){
      return id;
    }

    public boolean exclusive(){
      return exclusive;
    }

  }

}
