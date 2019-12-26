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

import java.util.Objects;
import java.util.Optional;

/**
 * Result returned from all kinds of Key-Value mutation operations.
 *
 * @since 3.0.0
 */
public class MutationResult {

  /**
   * Holds the CAS value of the document after the mutation.
   */
  private final long cas;

  /**
   * If returned, holds the mutation token of the document after the mutation.
   */
  private final Optional<MutationToken> mutationToken;

  /**
   * Creates a new {@link MutationResult}.
   *
   * @param cas the CAS value of the document after the mutation.
   * @param mutationToken the mutation token of the document after the mutation.
   */
  MutationResult(final long cas, final Optional<MutationToken> mutationToken) {
    this.cas = cas;
    this.mutationToken = mutationToken;
  }

  /**
   * Returns the CAS value of the document after the performed mutation.
   */
  public long cas() {
    return cas;
  }

  /**
   * Returns the {@link MutationToken} of the document after the performed mutation.
   *
   * <p>Note that this value is only present if mutation tokens have been enabled on the
   * environment configuration.</p>
   */
  public Optional<MutationToken> mutationToken() {
    return mutationToken;
  }

  @Override
  public String toString() {
    return "MutationResult{" +
      "cas=0x" + Long.toHexString(cas) +
      ", mutationToken=" + mutationToken +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    MutationResult that = (MutationResult) o;

    if (cas != that.cas) return false;
    return Objects.equals(mutationToken, that.mutationToken);
  }

  @Override
  public int hashCode() {
    int result = (int) (cas ^ (cas >>> 32));
    result = 31 * result + (mutationToken.isPresent() ? mutationToken.hashCode() : 0);
    return result;
  }

}
