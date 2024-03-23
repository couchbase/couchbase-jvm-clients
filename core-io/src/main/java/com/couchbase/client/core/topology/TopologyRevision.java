/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.topology;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Comparator;
import java.util.Objects;

@Stability.Internal
public class TopologyRevision implements Comparable<TopologyRevision> {

  /**
   * A synthetic revision, older than anything the server could send.
   * <p>
   * (Actually, the server could send a revision with a negative epoch
   * to indicate the epoch is not yet initialized, but we want
   * to ignore those undercooked configs.)
   */
  public static final TopologyRevision ZERO = new TopologyRevision(0, 0);

  private static final Comparator<TopologyRevision> comparator =
    Comparator.comparing(TopologyRevision::epoch)
      .thenComparing(TopologyRevision::rev);

  private final long epoch;
  private final long rev;

  public static TopologyRevision parse(ObjectNode json) {
    long epoch = json.path("revEpoch").longValue(); // zero if not present (server is too old to know about epochs)
    JsonNode revNode = json.path("rev");
    if (!revNode.isIntegralNumber()) {
      throw new IllegalArgumentException("Missing or non-integer 'rev' field.");
    }
    return new TopologyRevision(epoch, revNode.longValue());
  }

  /**
   * @param epoch May be negative to indicate the epoch is not yet initialized.
   * @param rev Never negative.
   */
  public TopologyRevision(long epoch, long rev) {
    // 'rev' is positive for all configs returned by the server.
    // Accept rev = 0 as well, so we can have our synthetic ZERO revision.
    if (rev < 0) {
      // The binary protocol docs describe the range of legal values.
      // Presumably, the same goes for the JSON representation.
      // https://github.com/couchbase/kv_engine/blob/master/docs/BinaryProtocol.md#0x01-clustermap-change-notification
      throw new IllegalArgumentException("Config revision must be non-negative, but got " + rev);
    }
    this.epoch = epoch;
    this.rev = rev;
  }

  public long epoch() {
    return epoch;
  }

  public long rev() {
    return rev;
  }

  @Override
  public int compareTo(TopologyRevision o) {
    return comparator.compare(this, o);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TopologyRevision that = (TopologyRevision) o;
    return epoch == that.epoch && rev == that.rev;
  }

  @Override
  public String toString() {
    return epoch + "." + rev;
  }

  @Override
  public int hashCode() {
    return Objects.hash(epoch, rev);
  }

  public boolean newerThan(TopologyRevision other) {
    return this.compareTo(other) > 0;
  }
}
