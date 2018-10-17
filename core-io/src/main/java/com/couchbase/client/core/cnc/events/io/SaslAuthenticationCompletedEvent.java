package com.couchbase.client.core.cnc.events.io;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.io.IoContext;

import java.time.Duration;

public class SaslAuthenticationCompletedEvent extends AbstractEvent {

  public SaslAuthenticationCompletedEvent(final Duration duration, final IoContext context) {
    super(Severity.DEBUG, Category.IO, duration, context);
  }

  @Override
  public String description() {
    return "SASL Authentication completed";
  }
}
