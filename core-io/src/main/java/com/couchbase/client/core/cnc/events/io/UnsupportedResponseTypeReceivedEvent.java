package com.couchbase.client.core.cnc.events.io;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;

import java.time.Duration;

public class UnsupportedResponseTypeReceivedEvent extends AbstractEvent {

  private final Object response;

  public UnsupportedResponseTypeReceivedEvent(IoContext context, Object response) {
    super(Severity.WARN, Category.IO, Duration.ZERO, context);
    this.response = response;
  }

  public Object response() {
    return response;
  }

  @Override
  public String description() {
    return "Received a response with an unsupported type. This is a bug! " + response;
  }
}
