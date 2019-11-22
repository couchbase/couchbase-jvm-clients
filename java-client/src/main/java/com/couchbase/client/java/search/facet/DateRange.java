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

package com.couchbase.client.java.search.facet;

import java.time.Instant;

public class DateRange {

  private final String name;
  public final String start;
  public final String end;

  private DateRange(String name, String start, String end) {
    this.name = name;
    this.start = start;
    this.end = end;
  }

  public static DateRange create(String name, String start, String end) {
    return new DateRange(name, start, end);
  }

  public static DateRange create(String name, Instant start, Instant end) {
    return new DateRange(
      name,
      start != null ? start.toString() : null,
      end != null ? end.toString() : null
    );
  }

  public String name() {
    return name;
  }

  public String start() {
    return start;
  }

  public String end() {
    return end;
  }

}