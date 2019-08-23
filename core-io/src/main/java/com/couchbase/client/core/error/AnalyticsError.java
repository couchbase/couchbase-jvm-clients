/*
 * Copyright 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.error;

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static com.couchbase.client.core.util.CbStrings.nullToEmpty;

public class AnalyticsError {

  public static final int FAILED_TO_PARSE_ERRORS = -1;

  public static final int DATAVERSE_NOT_FOUND = 24034;
  public static final int DATAVERSE_ALREADY_EXISTS = 24039;

  public static final int DATASET_NOT_FOUND = 24025;
  public static final int DATASET_ALREADY_EXISTS = 24040;

  public static final int INDEX_NOT_FOUND = 24047;
  public static final int INDEX_ALREADY_EXISTS = 24048;

  public static final int LINK_NOT_FOUND = 24006;

  private final int code;
  private final String message;
  private final Optional<String> query;

  public AnalyticsError(@JsonProperty("code") int code,
                        @JsonProperty("msg") String message,
                        @JsonProperty("query_from_user") String query) {
    this.code = code;
    this.message = nullToEmpty(message);
    this.query = Optional.ofNullable(query);
  }

  public int code() {
    return code;
  }

  public String message() {
    return message;
  }

  public Optional<String> query() {
    return query;
  }

  @Override
  public String toString() {
    return code + " " + message + (query.isPresent() ? " Query: " + query : "");
  }
}
