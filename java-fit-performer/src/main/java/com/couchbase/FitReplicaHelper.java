/*
 * Copyright 2026 Couchbase, Inc.
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

package com.couchbase;

import com.couchbase.client.java.kv.GetReplicaStrategy;
import com.couchbase.client.java.kv.ReplicaIndex;

public class FitReplicaHelper {
  private FitReplicaHelper() {
  }

  public static ReplicaIndex toSdk(com.couchbase.client.protocol.sdk.kv.replicas.ReplicaIndex fitReplicaIndex) {
    try {
      return ReplicaIndex.values()[fitReplicaIndex.getNumber()];
    } catch (IndexOutOfBoundsException e) {
      throw new UnsupportedOperationException("unrecognized replica index: " + fitReplicaIndex);
    }
  }

  public static GetReplicaStrategy toSdk(com.couchbase.client.protocol.sdk.kv.replicas.GetReplicaStrategy fitStrategy) {
    if (fitStrategy.hasFromIndex()) {
      var fitFromIndex = fitStrategy.getFromIndex();
      var result = GetReplicaStrategy.fromIndex(toSdk(fitFromIndex.getIndex()));
      if (fitFromIndex.hasOptions()) {
        var options = fitFromIndex.getOptions();
        if (options.hasWrap()) {
          result = result.withWrap(options.getWrap());
        }
      }
      return result;
    }

    throw new UnsupportedOperationException("unrecognized strategy: " + fitStrategy);
  }
}
