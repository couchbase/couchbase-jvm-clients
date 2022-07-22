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

package com.couchbase.client.core.kv;

import com.couchbase.client.core.util.Bytes;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

import static com.couchbase.client.core.util.Validators.notNull;

/**
 * Represents one individual document returned from a kv range scan stream.
 */
public class CoreRangeScanItem {

  private final int flags;
  private final Instant expiry;
  private final long seqno;
  private final long cas;
  private final String key;

  private final byte[] keyBytes;

  private final byte[] value;

  public static CoreRangeScanItem keyOnly(final byte[] key) {
    return new CoreRangeScanItem(0, null, 0, 0, key, Bytes.EMPTY_BYTE_ARRAY);
  }

  public static CoreRangeScanItem keyAndBody(final int flags, final Instant expiry, final long seqno, final long cas,
                                             final byte[] key, final byte[] value) {
    return new CoreRangeScanItem(flags, expiry, seqno, cas, key, value);
  }

  protected CoreRangeScanItem(final int flags, final Instant expiry, final long seqno, final long cas,
                              final byte[] key, final byte[] value) {
    this.flags = flags;
    this.expiry = expiry;
    this.seqno = seqno;
    this.cas = cas;
    this.keyBytes = key;
    this.key = new String(notNull(key, "RangeScanItem Key"), StandardCharsets.UTF_8);
    this.value = notNull(value, "RangeScanItem Value");
  }

  public int flags() {
    return flags;
  }

  public Instant expiry() {
    return expiry;
  }

  public long seqno() {
    return seqno;
  }

  public long cas() {
    return cas;
  }

  public String key() {
    return key;
  }

  public byte[] keyBytes() {
    return keyBytes;
  }

  public byte[] value() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CoreRangeScanItem that = (CoreRangeScanItem) o;
    return flags == that.flags && seqno == that.seqno && cas == that.cas && Objects.equals(expiry, that.expiry) && Objects.equals(key, that.key) && Arrays.equals(keyBytes, that.keyBytes) && Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(flags, expiry, seqno, cas, key);
    result = 31 * result + Arrays.hashCode(keyBytes);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  @Override
  public String toString() {
    return "CoreRangeScanItem{" +
      "flags=" + flags +
      ", expiry=" + expiry +
      ", seqno=" + seqno +
      ", cas=" + cas +
      ", key='" + key + '\'' +
      ", keyBytes=" + Arrays.toString(keyBytes) +
      ", value=" + Arrays.toString(value) +
      '}';
  }
}
