package com.couchbase.client.java.kv;

import com.couchbase.client.java.CommonOptions;

public class AppendOptions extends CommonOptions<AppendOptions> {
  public static AppendOptions DEFAULT = new AppendOptions();


  private long cas;
  private PersistTo persistTo;
  private ReplicateTo replicateTo;

  public long cas() {
    return cas;
  }

  public AppendOptions cas(long cas) {
    this.cas = cas;
    return this;
  }

  public PersistTo persistTo() {
    return persistTo;
  }

  public AppendOptions persistTo(final PersistTo persistTo) {
    this.persistTo = persistTo;
    return this;
  }

  public ReplicateTo replicateTo() {
    return replicateTo;
  }

  public AppendOptions replicateTo(final ReplicateTo replicateTo) {
    this.replicateTo = replicateTo;
    return this;
  }

}
