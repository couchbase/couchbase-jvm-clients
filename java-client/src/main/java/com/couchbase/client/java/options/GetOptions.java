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

package com.couchbase.client.java.options;

import com.couchbase.client.java.json.JsonObject;

import java.time.Duration;
import java.util.Optional;

public class GetOptions<T> {

  public static GetOptions<JsonObject> DEFAULT = GetOptions.create();

  private final Class<T> decodeInto;

  private final Optional<Duration> timeout;

  private GetOptions(Builder<T> builder) {
    this.decodeInto = builder.decodeInto;
    this.timeout = Optional.ofNullable(builder.timeout);
  }

  public static Builder<JsonObject> builder() {
    return builder(JsonObject.class);
  }

  public static <T> Builder<T> builder(final Class<T> decodeInto) {
    return new Builder<>(decodeInto);
  }

  public static GetOptions<JsonObject> create() {
    return builder().build();
  }

  public static <T> GetOptions<T> create(final Class<T> decodeInto) {
    return builder(decodeInto).build();
  }

  public Class<T> decodeInto() {
    return decodeInto;
  }

  public Optional<Duration> timeout() {
    return timeout;
  }

  public static class Builder<T> {

    private final Class<T> decodeInto;
    private Duration timeout = null;

    private Builder(Class<T> decodeInto) {
      this.decodeInto = decodeInto;
    }

    public Builder timeout(Duration timeout) {
      this.timeout = timeout;
      return this;
    }

    public GetOptions<T> build() {
      return new GetOptions<>(this);
    }
  }

}
