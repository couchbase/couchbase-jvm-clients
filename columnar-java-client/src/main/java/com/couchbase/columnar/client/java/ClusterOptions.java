/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.columnar.client.java;

import com.couchbase.columnar.client.java.codec.Deserializer;
import com.couchbase.columnar.client.java.internal.DefaultJacksonDeserializerHolder;
import reactor.util.annotation.Nullable;

import java.util.function.Consumer;

public final class ClusterOptions {
  boolean srv = true;
  @Nullable Deserializer deserializer;
  final SecurityOptions security = new SecurityOptions();
  final TimeoutOptions timeout = new TimeoutOptions();

  ClusterOptions() {
  }

  Unmodifiable build() {
    return new Unmodifiable(this);
  }

  /**
   * Specifies whether the SDK should treat the connection string address
   * as a DNS SRV record. Defaults to true.
   */
  public ClusterOptions srv(boolean useDnsSrv) {
    this.srv = useDnsSrv;
    return this;
  }

  /**
   * Sets the default deserializer for converting query result rows into Java objects.
   * <p>
   * If not specified, the SDK uses an instance of {@code JacksonDeserializer}
   * backed by a JsonMapper.
   * <p>
   * For complete control over the data conversion process,
   * provide your own custom {@link Deserializer} implementation.
   * <p>
   * Can be overridden on a per-query basis by calling
   * {@link QueryOptions#deserializer(Deserializer)}.
   */
  public ClusterOptions deserializer(@Nullable Deserializer deserializer) {
    this.deserializer = deserializer;
    return this;
  }

  public ClusterOptions security(Consumer<SecurityOptions> optionsCustomizer) {
    optionsCustomizer.accept(this.security);
    return this;
  }

  public ClusterOptions timeout(Consumer<TimeoutOptions> optionsCustomizer) {
    optionsCustomizer.accept(this.timeout);
    return this;
  }

  static class Unmodifiable {
    private final TimeoutOptions.Unmodifiable timeout;
    private final SecurityOptions.Unmodifiable security;
    private final Deserializer deserializer;
    private final boolean srv;

    Unmodifiable(ClusterOptions builder) {
      this.timeout = builder.timeout.build();
      this.security = builder.security.build();
      this.srv = builder.srv;

      this.deserializer = builder.deserializer != null
        ? builder.deserializer
        : DefaultJacksonDeserializerHolder.DESERIALIZER; // triggers class loading, which fails if user excluded Jackson
    }

    public TimeoutOptions.Unmodifiable timeout() {
      return timeout;
    }

    public SecurityOptions.Unmodifiable security() {
      return security;
    }

    public Deserializer deserializer() {
      return deserializer;
    }

    public boolean srv() {
      return srv;
    }

    @Override
    public String toString() {
      return "ClusterOptions{" +
        "timeout=" + timeout +
        ", security=" + security +
        ", deserializer=" + deserializer +
        ", srv=" + srv +
        '}';
    }

  }
}
