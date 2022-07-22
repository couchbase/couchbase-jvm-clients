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

import java.nio.charset.StandardCharsets;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * A single {@link ScanTerm} identifying either the point to scan from or to scan to.
 */
@Stability.Volatile
public class ScanTerm {

  /**
   * The minimum scan pattern - construct with {@link #minimum()}.
   */
  public static final byte[] MINIMUM_PATTERN = new byte[]{ 0x00 };

  /**
   * The maximum scan pattern - construct with {@link #maximum()} ()}.
   */
  public static final byte[] MAXIMUM_PATTERN = new byte[]{ (byte) 0xFF };

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
    this.id = notNullOrEmpty(id, "ScanTerm ID");
    this.exclusive = exclusive;
  }

  public byte[] id() {
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
    return inclusive(id.getBytes(StandardCharsets.UTF_8));
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
    return exclusive(id.getBytes(StandardCharsets.UTF_8));
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
   * Creates a {@link ScanTerm} representing the absolute minimum pattern (starting point) - see {@link ScanTerm#MINIMUM_PATTERN}.
   *
   * @return the created {@link ScanTerm}.
   */
  public static ScanTerm minimum() {
    return new ScanTerm(MINIMUM_PATTERN, false);
  }

  /**
   * Creates a {@link ScanTerm} representing the absolute maximum pattern (end point) - see {@link ScanTerm#MAXIMUM_PATTERN}.
   *
   * @return the created {@link ScanTerm}.
   */
  public static ScanTerm maximum() {
    return new ScanTerm(MAXIMUM_PATTERN, false);
  }

}