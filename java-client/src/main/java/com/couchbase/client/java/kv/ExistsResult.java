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

/**
 * Result returned from an exists KeyValue operation.
 *
 * @since 3.0.0
 */
public class ExistsResult {

  /**
   * Holds the CAS value of the doc if it exists.
   */
  private final long cas;

  /**
   * Creates a new {@link ExistsResult}.
   *
   * @param cas the CAS of the document.
   */
  ExistsResult(long cas) {
    this.cas = cas;
  }

  /**
   * If the document is present, returns its current CAS value at the time of the exists operation.
   */
  public long cas() {
    return cas;
  }

  @Override
  public String toString() {
    return "ExistsResult{" +
      "cas=" + cas +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ExistsResult that = (ExistsResult) o;

    return cas == that.cas;
  }

  @Override
  public int hashCode() {
    return (int) (cas ^ (cas >>> 32));
  }

}
