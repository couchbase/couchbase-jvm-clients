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
package com.couchbase.client.core.cnc.events.tracing;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.json.Mapper;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Emits orphaned requests which can be used to analyze slow requests.
 */
public class OrphansRecordedEvent extends AbstractEvent {

  final Map<String, Object> orphansNew;
  final List<Map<String, Object>> orphansOld;

  public OrphansRecordedEvent(final Duration duration, final Map<String, Object> orphansNew, final List<Map<String, Object>> orphansOld) {
    super(Severity.WARN, Category.TRACING, duration, null);
    this.orphansNew = orphansNew;
    this.orphansOld = orphansOld;
  }

  @Deprecated
  public List<Map<String, Object>> orphans() {
    return orphansOld;
  }

  @Override
  public String description() {
    return "Orphaned requests found: " + Mapper.encodeAsString(orphansNew == null ? orphansOld : orphansNew);
  }

}
