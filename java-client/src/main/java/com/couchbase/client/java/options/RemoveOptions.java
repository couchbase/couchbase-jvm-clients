package com.couchbase.client.java.options;

import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

public class RemoveOptions {

  public static RemoveOptions DEFAULT = RemoveOptions.create();

  private final Optional<Duration> timeout;
  private final PersistTo persistTo;
  private final ReplicateTo replicateTo;
  private final long cas;

  private RemoveOptions(Builder builder) {
    this.timeout = Optional.ofNullable(builder.timeout);
    this.persistTo = builder.persistTo;
    this.replicateTo = builder.replicateTo;
    this.cas = builder.cas;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static RemoveOptions create() {
    return new Builder().build();
  }

  public Optional<Duration> timeout() {
    return timeout;
  }


  public long cas() {
    return cas;
  }

  public PersistTo persistTo() {
    return persistTo;
  }

  public ReplicateTo replicateTo() {
    return replicateTo;
  }

  public static class Builder {

    private Duration timeout = null;
    private PersistTo persistTo;
    private ReplicateTo replicateTo;
    private long cas;

    private Builder() {
    }

    public Builder timeout(final Duration timeout) {
      this.timeout = timeout;
      return this;
    }

    public Builder cas(final long cas) {
      this.cas = cas;
      return this;
    }

    public Builder persistTo(final PersistTo persistTo) {
      this.persistTo = persistTo;
      return this;
    }

    public Builder replicateTo(final ReplicateTo replicateTo) {
      this.replicateTo = replicateTo;
      return this;
    }

    public RemoveOptions build() {
      return new RemoveOptions(this);
    }

  }
}
