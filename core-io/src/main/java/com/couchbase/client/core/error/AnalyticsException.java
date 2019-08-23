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

package com.couchbase.client.core.error;

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.json.Mapper;

import java.util.List;

import static com.couchbase.client.core.error.AnalyticsError.FAILED_TO_PARSE_ERRORS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

/**
 * There was a problem fulfilling the analytics request.
 * <p>
 * Check <code>errors()</code> for further details.
 *
 * @since 2.0.0
 */
public class AnalyticsException extends CouchbaseException {
  private final List<AnalyticsError> errors;
  private final byte[] content;

  protected AnalyticsException(AnalyticsException cause) {
    super(cause.getMessage(), cause);
    this.content = cause.content;
    this.errors = cause.errors;
  }

  public AnalyticsException(byte[] content) {
    this(content, null);
  }

  public AnalyticsException(byte[] content, Throwable cause) {
    super("Analytics Query Failed: " + new String(content, UTF_8), cause);

    this.content = requireNonNull(content);

    List<AnalyticsError> tempErrors;
    try {
      tempErrors = unmodifiableList(Mapper.decodeInto(content, new TypeReference<List<AnalyticsError>>() {
      }));
    } catch (Exception e) {
      tempErrors = singletonList(new AnalyticsError(FAILED_TO_PARSE_ERRORS, new String(content, UTF_8), null));
    }

    this.errors = tempErrors;
  }

  public byte[] content() {
    return content;
  }

  public List<AnalyticsError> errors() {
    return errors;
  }

  public boolean hasErrorCode(int code) {
    return errors.stream().anyMatch(e -> e.code() == code);
  }
}
