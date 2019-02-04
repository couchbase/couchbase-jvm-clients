package com.couchbase.client.core.cnc.events.io;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.io.IoContext;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import java.time.Duration;

public class UnknownResponseReceivedEvent extends AbstractEvent {

  private final byte[] response;

  public UnknownResponseReceivedEvent(IoContext context, byte[] response) {
    super(Severity.WARN, Category.IO, Duration.ZERO, context);
    this.response = response;
  }

  public byte[] response() {
    return response;
  }

  @Override
  public String description() {
    return "Received a response with no matching opaque/request: \n"
      + ByteBufUtil.prettyHexDump(Unpooled.copiedBuffer(response))
      + "\n";
  }
}
