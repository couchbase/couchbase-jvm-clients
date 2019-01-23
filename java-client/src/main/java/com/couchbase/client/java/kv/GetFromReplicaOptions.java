package com.couchbase.client.java.kv;

import com.couchbase.client.java.CommonOptions;

public class GetFromReplicaOptions extends CommonOptions<GetFromReplicaOptions> {
  public static GetFromReplicaOptions DEFAULT = new GetFromReplicaOptions();

  private ReplicaMode replicaMode = ReplicaMode.ALL;

  public static GetFromReplicaOptions getFromReplicaOptions() {
    return new GetFromReplicaOptions();
  }

  private GetFromReplicaOptions() {
  }

  public ReplicaMode replicaMode() {
    return replicaMode;
  }

  public GetFromReplicaOptions replicaMode(final ReplicaMode replicaMode) {
    this.replicaMode = replicaMode;
    return this;
  }

}
