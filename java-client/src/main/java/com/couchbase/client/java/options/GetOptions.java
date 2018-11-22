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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.json.JsonObject;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Allows to customize a get request.
 */
public class GetOptions {

  /**
   * The default options, used most of the time.
   */
  public static final GetOptions DEFAULT = new GetOptions();

  /**
   * Optionally set if a custom timeout is provided.
   */
  private Duration timeout;

  private final Map<String, FieldType> fields;

  /**
   * Creates a new set of {@link GetOptions} with a {@link JsonObject} target.
   *
   * @return options to customize.
   */
  public static GetOptions getOptions() {
    return new GetOptions();
  }


  private GetOptions() {
    fields = new HashMap<>();
  }

  public GetOptions timeout(final Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  public Duration timeout() {
    return timeout;
  }

  public GetOptions field(String path) {
    fields.put(path, FieldType.GET);
    return this;
  }

  public GetOptions fieldExists(String path) {
    fields.put(path, FieldType.EXISTS);
    return this;
  }

  public GetOptions fields(String... paths) {
    for (String p : paths) {
      field(p);
    }
    return this;
  }

  public GetOptions fieldsExist(String... paths) {
    for (String p : paths) {
      fieldExists(p);
    }
    return this;
  }

  @Stability.Internal
  public Map<String, FieldType> fields() {
    return fields;
  }

  enum FieldType {
    GET,
    EXISTS
  }
}
