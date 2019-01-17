package com.couchbase.client.core.cnc.events.node;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.AbstractEvent;

import java.time.Duration;

public class NodePartitionLengthNotEqualEvent extends AbstractEvent {

  private final int actualSize;
  private final int configSize;

  public NodePartitionLengthNotEqualEvent(final CoreContext context, final int actualSize,
                                          final int configSize) {
    super(Severity.DEBUG, Category.NODE, Duration.ZERO, context);
    this.actualSize = actualSize;
    this.configSize = configSize;
  }

  @Override
  public String description() {
    return String.format("Node list and configuration's partition hosts sizes are not equal. " +
      "%s vs. %s", actualSize, configSize);
  }
}
