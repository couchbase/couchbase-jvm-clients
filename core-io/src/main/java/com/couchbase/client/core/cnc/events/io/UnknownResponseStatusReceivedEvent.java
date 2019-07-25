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

package com.couchbase.client.core.cnc.events.io;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.io.IoContext;

import java.time.Duration;

public class UnknownResponseStatusReceivedEvent extends AbstractEvent {

  private final short status;

  public UnknownResponseStatusReceivedEvent(IoContext context, short status) {
    super(Severity.INFO, Category.IO, Duration.ZERO, context);
    this.status = status;
  }

  public short status() {
    return status;
  }

  @Override
  public String description() {
    return "Received an unknown response status: 0x" + Integer.toHexString(status);
  }

}
