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
import java.util.function.Function;

/**
 * Allows to customize a get request.
 *
 * @param <T> the type into which the response will be converted.
 */
public class GetOptions<T> {

  /**
   * The default options, used most of the time.
   */
  public static final GetOptions<JsonObject> DEFAULT = new GetOptions<>(JsonObject.class);

  /**
   * The selected target class for a get request to decode into.
   */
  private final Class<T> target;

  /**
   * Optionally set if a custom timeout is provided.
   */
  private Duration timeout;

  /**
   * Optionally set if a custom decoder for the target is provided.
   */
  private Function<byte[], T> decoder;

  /**
   * Creates a new set of {@link GetOptions} with a {@link JsonObject} target.
   *
   * @return options to customize.
   */
  public static GetOptions<JsonObject> getOptions() {
    return getOptions(JsonObject.class);
  }

  /**
   * Creates a new set of {@link GetOptions} with a custom target.
   *
   * @param target the custom target.
   * @param <T> the generic custom target type.
   * @return options to customize.
   */
  public static <T> GetOptions<T> getOptions(final Class<T> target) {
    return new GetOptions<>(target);
  }

  private GetOptions(final Class<T> target) {
    this.target = target;
  }

  public Class<T> target() {
    return target;
  }

  public GetOptions<T> timeout(final Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  public Duration timeout() {
    return timeout;
  }

  public Function<byte[], T> decoder() {
    return decoder;
  }

  public GetOptions<T> decoder(final Function<byte[], T> decoder) {
    this.decoder = decoder;
    return this;
  }

}
