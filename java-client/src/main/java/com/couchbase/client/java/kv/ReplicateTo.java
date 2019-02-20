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

import com.couchbase.client.core.service.kv.Observe;

// TODO add this: Adding additional enumeration values for MASTER_PLUS_ONE, MASTER_PLUS_TWO, MASTER_PLUS_THREE.  Overall enumeration values would be MASTER, MASTER_PLUS_ONE, MASTER_PLUS_TWO, MASTER_PLUS_THREE, ONE, TWO, THREE, FOUR.

public enum ReplicateTo {
  NONE(Observe.ObserveReplicateTo.NONE),
  ONE(Observe.ObserveReplicateTo.ONE),
  TWO(Observe.ObserveReplicateTo.TWO),
  THREE(Observe.ObserveReplicateTo.THREE);

  private final Observe.ObserveReplicateTo coreHandle;

  ReplicateTo(Observe.ObserveReplicateTo coreHandle) {
    this.coreHandle = coreHandle;
  }

  public Observe.ObserveReplicateTo coreHandle() {
    return coreHandle;
  }
}
