/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.core.cnc.events.io;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.io.IoContext;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class UnknownServerPushRequestReceivedEvent extends AbstractEvent {

  private final byte[] request;

  public UnknownServerPushRequestReceivedEvent(IoContext context, byte[] request) {
    super(Severity.WARN, Category.IO, Duration.ZERO, context);
    this.request = requireNonNull(request);
  }

  public byte[] request() {
    return request;
  }

  @Override
  public String description() {
    return "Received a server push request with an unsupported opcode: \n"
      + ByteBufUtil.prettyHexDump(Unpooled.wrappedBuffer(request))
      + "\n";
  }
}
