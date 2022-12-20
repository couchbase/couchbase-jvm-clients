/*
 * Copyright (c) 2018 Couchbase, Inc.
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
import com.couchbase.client.core.api.kv.CoreExistsResult;
import com.couchbase.client.core.error.CasMismatchException;

import java.util.Objects;

/**
 * Result returned from an exists KeyValue operation.
 *
 * @since 3.0.0
 */
public class ExistsResult {
  private static final ExistsResult NOT_FOUND = new ExistsResult(false, 0);

  /**
   * Holds the CAS value of the doc if it exists.
   */
  private final long cas;

  /**
   * Holds the boolean if the doc actually exists.
   */
  private final boolean exists;

  /**
   * Creates a new {@link ExistsResult}.
   *
   * @param exists if the doc exists or not.
   * @param cas the CAS of the document.
   */
  ExistsResult(final boolean exists, final long cas) {
    this.exists = exists;
    this.cas = cas;
  }

  @Stability.Internal
  public static ExistsResult from(CoreExistsResult it) {
    return it.exists() ? new ExistsResult(true, it.cas()) : NOT_FOUND;
  }

  /**
   * Returns the CAS value of document at the time of loading.
   * <p>
   * Note that if the document does not exist, this will return 0!
   * <p>
   * The CAS value is an opaque identifier which is associated with a specific state of the document on the server. It
   * can be used during a subsequent mutation to make sure that the document has not been modified in the meantime.
   * <p>
   * If document on the server has been modified in the meantime the SDK will raise a {@link CasMismatchException}. In
   * this case the caller is expected to re-do the whole "fetch-modify-update" cycle again. Please refer to the
   * SDK documentation for more information on CAS mismatches and subsequent retries.
   */
  public long cas() {
    return cas;
  }

  /**
   * True if the document exists, false otherwise.
   */
  public boolean exists() {
    return exists;
  }

  @Override
  public String toString() {
    return "ExistsResult{" +
      "cas=0x" + Long.toHexString(cas) +
      ", exists=" + exists +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ExistsResult that = (ExistsResult) o;
    return cas == that.cas && exists == that.exists;
  }

  @Override
  public int hashCode() {
    return Objects.hash(cas, exists);
  }
}
