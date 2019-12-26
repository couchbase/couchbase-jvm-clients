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

import com.couchbase.client.core.msg.kv.MutationToken;

import java.util.Optional;

/**
 * Result returned from counter (increment, decrement) operations.
 *
 * @since 3.0.0
 */
public class CounterResult extends MutationResult {

  /**
   * Holds the new counter returned after the mutation.
   */
  private final long content;

  /**
   * Creates a new {@link CounterResult}.
   *
   * @param cas the new CAS value for the document.
   * @param content the new content after the counter operation.
   * @param mutationToken the new mutation token for the document, if present.
   */
  CounterResult(final long cas, final long content, final Optional<MutationToken> mutationToken) {
    super(cas, mutationToken);
    this.content = content;
  }

  /**
   * The new counter value after the mutation has been performed.
   *
   * @return the modified counter value.
   */
  public long content() {
    return content;
  }

  @Override
  public String toString() {
    return "CounterResult{" +
      "cas=0x" + Long.toHexString(cas()) +
      ", mutationToken=" + mutationToken() +
      ", content=" + content +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    CounterResult that = (CounterResult) o;

    return content == that.content;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (int) (content ^ (content >>> 32));
    return result;
  }

}
