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

import java.time.Duration;
import java.util.Optional;

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
