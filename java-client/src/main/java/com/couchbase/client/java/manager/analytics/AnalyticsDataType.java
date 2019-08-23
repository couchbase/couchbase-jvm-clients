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

package com.couchbase.client.java.manager.analytics;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * The indexable data types.
 */
public class AnalyticsDataType {
  public static final AnalyticsDataType STRING = new AnalyticsDataType("string");
  public static final AnalyticsDataType INT64 = new AnalyticsDataType("int64");
  public static final AnalyticsDataType DOUBLE = new AnalyticsDataType("double");

  private final String value;

  private AnalyticsDataType(String value) {
    this.value = requireNonNull(value);
  }

  /**
   * If there's no pre-defined constant for a data type, you can create your own using this method.
   */
  public static AnalyticsDataType valueOf(String value) {
    return new AnalyticsDataType(value);
  }

  public String value() {
    return value;
  }

  @Override
  public String toString() {
    return value();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AnalyticsDataType that = (AnalyticsDataType) o;
    return value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
