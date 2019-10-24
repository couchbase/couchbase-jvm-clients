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

  final List<Map<String, Object>> overThreshold;

  public OverThresholdRequestsRecordedEvent(final Duration duration, final List<Map<String, Object>> overThreshold) {
    super(Severity.WARN, Category.TRACING, duration, null);
    this.overThreshold = overThreshold;
  }

  public List<Map<String, Object>> overThreshold() {
    return overThreshold;
  }

  @Override
  public String description() {
    return "Requests over Threshold found: " + Mapper.encodeAsString(overThreshold);
  }

}
