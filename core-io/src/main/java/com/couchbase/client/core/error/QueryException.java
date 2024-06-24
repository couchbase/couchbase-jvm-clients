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

import com.couchbase.client.core.annotation.UsedBy;
import com.couchbase.client.core.util.CbCollections;

import java.util.Collection;
import java.util.List;

import static com.couchbase.client.core.annotation.UsedBy.Project.SPRING_DATA_COUCHBASE;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * There was a problem fulfilling the query request.
 * <p>
 * Check {@link #errors()} for further details.
 *
 * @since 2.0.0
 * @deprecated The SDK never throws this exception.
 */
@Deprecated
@UsedBy(SPRING_DATA_COUCHBASE) // Used only in SDC JavaIntegrationTests, so probably safe to remove from SDC too.
public class QueryException extends CouchbaseException {
  private final List<ErrorCodeAndMessage> errors;

  public QueryException(QueryException cause) {
    super(cause);
    this.errors = cause.errors();
  }

  public QueryException(final byte[] content) {
    super("Query Failed: " + new String(content, UTF_8));
    this.errors = ErrorCodeAndMessage.fromJsonArray(content);
  }

  public QueryException(String message, Collection<ErrorCodeAndMessage> errors) {
    super("Query Failed: " + message);
    this.errors = CbCollections.copyToUnmodifiableList(errors);
  }

  /**
   * Returns a human-readable description of the error, as reported by the query service.
   */
  public String msg() {
    return errors.isEmpty() ? getMessage() : errors.get(0).message();
  }

  /**
   * Returns the numeric error code from the query service.
   * <p>
   * These are detailed in the
   * <a href="https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/n1ql-error-codes.html">
   * N1QL Error Codes documentation</a>.
   */
  public int code() {
    return errors.isEmpty() ? 0 : errors().get(0).code();
  }

  /**
   * Returns the full list of errors and warnings associated with the exception.
   * Possible error codes are detailed in the
   * <a href="https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/n1ql-error-codes.html">
   * N1QL Error Codes documentation</a>.
   */
  public List<ErrorCodeAndMessage> errors() {
    return errors;
  }

  public boolean hasErrorCode(int code) {
    return errors.stream().anyMatch(e -> e.code() == code);
  }
}
