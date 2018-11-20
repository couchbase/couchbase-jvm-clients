package com.couchbase.client.java.options;

import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

public class ReplaceOptions<T> {

  public static ReplaceOptions<Object> DEFAULT = ReplaceOptions.create();

  private Function<T, byte[]> encoder;
  private final Optional<Duration> timeout;
  private final Optional<Duration> expiry;
  private final PersistTo persistTo;
  private final ReplicateTo replicateTo;
  private final long cas;

  private ReplaceOptions(Builder<T> builder) {
    this.encoder = builder.encoder;
    this.timeout = Optional.ofNullable(builder.timeout);
    this.expiry = Optional.ofNullable(builder.expiry);
    this.persistTo = builder.persistTo;
    this.replicateTo = builder.replicateTo;
    this.cas = builder.cas;
  }

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  public static <T> ReplaceOptions<T> create() {
    return new Builder<T>().build();
  }

  public Function<T, byte[]> encoder() {
    return encoder;
  }

  public Optional<Duration> timeout() {
    return timeout;
  }

  public Optional<Duration> expiry() {
    return expiry;
  }

  public long cas() {
    return cas;
  }

  public static class Builder<T> {

    private Function<T, byte[]> encoder;
    private Duration timeout = null;
    private Duration expiry = null;
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

    public Builder expiry(final Duration expiry) {
      this.expiry = expiry;
      return this;
    }

    public Builder encoder(final Function<T, byte[]> encoder) {
      this.encoder = encoder;
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

    public ReplaceOptions<T> build() {
      return new ReplaceOptions<>(this);
    }

  }
}
