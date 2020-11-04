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
import com.couchbase.client.core.cnc.Context;
import com.couchbase.client.core.json.Mapper;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Emits the over threshold requests which can be used to analyze slow requests.
 */
public class OverThresholdRequestsRecordedEvent extends AbstractEvent {

  final Map<String, Object> overThresholdNew;
  final List<Map<String, Object>> overThresholdOld;

  public OverThresholdRequestsRecordedEvent(final Duration duration, final Map<String, Object> overThresholdNew,
                                            List<Map<String, Object>> overThresholdOld) {
    super(Severity.WARN, Category.TRACING, duration, null);
    this.overThresholdNew = overThresholdNew;
    this.overThresholdOld = overThresholdOld;
  }

  @Deprecated
  public List<Map<String, Object>> overThreshold() {
    return overThresholdOld;
  }

  @Override
  public String description() {
    return "Requests over Threshold found: " + Mapper.encodeAsString(overThresholdOld == null ? overThresholdNew : overThresholdOld);
  }

}
