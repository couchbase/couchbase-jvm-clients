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

import com.couchbase.client.core.annotation.Stability;

import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * There was a problem fulfilling the analytics request.
 * <p>
 * Check {@link #errors()} for further details.
 *
 * @since 2.0.0
 */
public class AnalyticsException extends CouchbaseException {
  private final List<ErrorCodeAndMessage> errors;
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
    this.errors = ErrorCodeAndMessage.fromJsonArray(content);
  }

  @Stability.Internal
  public byte[] content() {
    return content;
  }

  /**
   * Returns the full list of errors and warnings associated with the exception.
   * Possible error codes are detailed in the
   * <a href="https://docs.couchbase.com/server/current/analytics/error-codes.html">
   * Analytics Error Codes documentation</a>.
   */
  public List<ErrorCodeAndMessage> errors() {
    return errors;
  }

  public boolean hasErrorCode(int code) {
    return errors.stream().anyMatch(e -> e.code() == code);
  }
}
