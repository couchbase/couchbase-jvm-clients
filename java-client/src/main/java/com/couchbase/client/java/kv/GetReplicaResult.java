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
import com.couchbase.client.core.api.kv.CoreGetResult;
import com.couchbase.client.core.msg.kv.GetResponse;
import com.couchbase.client.core.service.kv.ReplicaHelper;
import com.couchbase.client.java.codec.Transcoder;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

/**
 * Extends GetResult to include additional information for get-from-replica style calls.
 *
 * @since 3.0.0
 */
public class GetReplicaResult extends GetResult {

  /**
   * True if this result came from a replica.
   */
  private final boolean isReplica;

  /**
   * Creates a new {@link GetReplicaResult}.
   *
   * @param cas the cas from the doc.
   * @param expiry the expiration if fetched from the doc.
   * @param isReplica whether the active or replica returned this result
   */
  private GetReplicaResult(final byte[] content, final int flags, final long cas, final Optional<Instant> expiry,
                   final Transcoder transcoder, boolean isReplica) {
    super(content, flags, cas, expiry, transcoder);
    this.isReplica = isReplica;
  }

  @Stability.Internal
  public static GetReplicaResult from(ReplicaHelper.GetReplicaResponse response, Transcoder transcoder) {
    GetResponse get = response.getResponse();
    return new GetReplicaResult(
        get.content(),
        get.flags(),
        get.cas(),
        Optional.empty(),
        transcoder,
        response.isFromReplica()
    );
  }

  @Stability.Internal
  public static GetReplicaResult from(CoreGetResult get, Transcoder transcoder) {
    return new GetReplicaResult(
      get.content(),
      get.flags(),
      get.cas(),
      Optional.empty(),
      transcoder,
      get.replica()
    );
  }

  /**
   * Returns whether the replica that returned this result was the replica or the active.
   */
  public boolean isReplica() {
    return isReplica;
  }

  @Override
  public String toString() {
    return "GetReplicaResult{" +
      "content=" + redactUser(convertContentToString(content, flags)) +
      ", flags=0x" + Integer.toHexString(flags) +
      ", cas=0x" + Long.toHexString(cas()) +
      ", expiry=" + expiryTime() +
      ", isReplica=" + isReplica +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    GetReplicaResult getResult = (GetReplicaResult) o;

    if (isReplica != getResult.isReplica) return false;
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(content, flags, cas(), expiry(), isReplica);
  }
}
