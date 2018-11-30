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

import com.couchbase.client.java.json.JsonObject;

import java.time.Duration;

/**
 * Allows to customize a get request.
 */
public class ReadOptions {

  /**
   * The default options, used most of the time.
   */
  public static final ReadOptions DEFAULT = new ReadOptions();

  /**
   * Optionally set if a custom timeout is provided.
   */
  private Duration timeout;

  /**
   * If the expiration should also fetched with a get.
   */
  private boolean withExpiration;

  /**
   * Creates a new set of {@link ReadOptions} with a {@link JsonObject} target.
   *
   * @return options to customize.
   */
  public static ReadOptions readOptions() {
    return new ReadOptions();
  }

  private ReadOptions() {
    withExpiration = false;
  }

  public ReadOptions timeout(final Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  public Duration timeout() {
    return timeout;
  }

  public ReadOptions withExpiration(boolean expiration) {
    withExpiration = true;
    return this;
  }

  public boolean withExpiration() {
    return withExpiration;
  }

}
