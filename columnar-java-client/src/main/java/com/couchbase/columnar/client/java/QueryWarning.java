/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.columnar.client.java;

import com.couchbase.client.core.error.ErrorCodeAndMessage;

import static java.util.Objects.requireNonNull;

/**
 * Represents a single warning returned from the analytics engine.
 * <p>
 * Note that warnings are not terminal errors, but hints from the engine that something went not as expected.
 */
public final class QueryWarning {

  private final ErrorCodeAndMessage inner;

  QueryWarning(final ErrorCodeAndMessage inner) {
    this.inner = requireNonNull(inner);
  }

  public int code() {
    return inner.code();
  }

  /**
   * <b>Caveat:</b> The content and structure of this message may change
   * between Couchbase Server versions.
   */
  public String message() {
    return inner.message();
  }

  @Override
  public String toString() {
    return inner.toString();
  }

}
