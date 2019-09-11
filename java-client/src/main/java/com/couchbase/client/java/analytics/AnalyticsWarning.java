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

package com.couchbase.client.java.analytics;

import com.couchbase.client.core.error.ErrorCodeAndMessage;

/**
 * Represents a single warning returned from the analytics engine.
 *
 * <p>Note that warnings are not terminal errors but hints from the engine that something went not as expected.</p>
 */
public class AnalyticsWarning {

  private final ErrorCodeAndMessage inner;

  AnalyticsWarning(final ErrorCodeAndMessage inner) {
    this.inner = inner;
  }

  public int code() {
    return inner.code();
  }

  public String message() {
    return inner.message();
  }

  @Override
  public String toString() {
    return "AnalyticsWarning{" +
      "inner=" + inner +
      '}';
  }

}
