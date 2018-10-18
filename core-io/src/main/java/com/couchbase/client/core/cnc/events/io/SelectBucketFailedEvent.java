package com.couchbase.client.core.cnc.events.io;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.io.IoContext;

import java.time.Duration;

public class SelectBucketFailedEvent extends AbstractEvent {

  /**
   * Holds the the description for this select bucket failed event.
   */
  private final short status;

  public SelectBucketFailedEvent(final IoContext context, final short status) {
    super(Severity.ERROR, Category.IO, Duration.ZERO, context);
    this.status = status;
  }

  public short status() {
    return status;
  }

  @Override
  public String description() {
    return "Select bucket failed with status code 0x" + Integer.toHexString(status);
  }
}
