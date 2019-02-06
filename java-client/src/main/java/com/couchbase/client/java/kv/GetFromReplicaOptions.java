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
import com.couchbase.client.java.CommonOptions;

public class GetFromReplicaOptions extends CommonOptions<GetFromReplicaOptions> {
  public static GetFromReplicaOptions DEFAULT = new GetFromReplicaOptions();

  private ReplicaMode replicaMode = ReplicaMode.ALL;

  public static GetFromReplicaOptions getFromReplicaOptions() {
    return new GetFromReplicaOptions();
  }

  private GetFromReplicaOptions() {
  }

  public GetFromReplicaOptions replicaMode(final ReplicaMode replicaMode) {
    this.replicaMode = replicaMode;
    return this;
  }

  @Stability.Internal
  public BuiltGetFromReplicaOptions build() {
    return new BuiltGetFromReplicaOptions();
  }

  public class BuiltGetFromReplicaOptions extends BuiltCommonOptions {

    public ReplicaMode replicaMode() {
      return replicaMode;
    }

  }

}
