/*
 * Copyright (c) 2023 Couchbase, Inc.
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
import com.couchbase.client.core.api.kv.CoreSubdocGetResult;
import com.couchbase.client.java.codec.JsonSerializer;

/**
 * Extends LookupInResult to include additional information for lookupIn-from-replica style calls.
 */
public class LookupInReplicaResult extends LookupInResult {

  /**
   * True if this result came from a replica.
   */
  private final boolean isReplica;

  /**
   * Creates a new {@link LookupInReplicaResult}.
   *
   * @param response
   * @param serializer
   * @param isReplica whether the active or replica returned this result
   */
  @Stability.Internal
  private LookupInReplicaResult(CoreSubdocGetResult response, JsonSerializer serializer, boolean isReplica) {
    super(response, serializer);
    this.isReplica = isReplica;
  }

  @Stability.Internal
  public static LookupInReplicaResult from(CoreSubdocGetResult response, JsonSerializer serializer) {
    return new LookupInReplicaResult(
      response,
      serializer,
      response.replica()
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
    return "LookupInReplicaResult{" +
      super.toString() +
      ", isReplica=" + isReplica +
      '}';
  }

}
