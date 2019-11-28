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
