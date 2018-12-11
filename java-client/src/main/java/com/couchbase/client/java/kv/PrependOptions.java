package com.couchbase.client.java.kv;

import com.couchbase.client.java.CommonOptions;

public class PrependOptions extends CommonOptions<PrependOptions> {
  public static PrependOptions DEFAULT = new PrependOptions();


  private long cas;
  private PersistTo persistTo;
  private ReplicateTo replicateTo;

  public long cas() {
    return cas;
  }

  public PrependOptions cas(long cas) {
    this.cas = cas;
    return this;
  }

  public PersistTo persistTo() {
    return persistTo;
  }

  public PrependOptions persistTo(final PersistTo persistTo) {
    this.persistTo = persistTo;
    return this;
  }

  public ReplicateTo replicateTo() {
    return replicateTo;
  }

  public PrependOptions replicateTo(final ReplicateTo replicateTo) {
    this.replicateTo = replicateTo;
    return this;
  }

}
