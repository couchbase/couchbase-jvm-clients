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
import com.couchbase.client.core.io.netty.kv.ErrorMap;

import java.time.Duration;

/**
 * Raised if an unknown error is decoded and handled through the KV error map.
 */
public class KeyValueErrorMapCodeHandledEvent extends AbstractEvent {

  private final ErrorMap.ErrorCode errorCode;

  public KeyValueErrorMapCodeHandledEvent(final IoContext context, final ErrorMap.ErrorCode errorCode) {
    super(Severity.DEBUG, Category.IO, Duration.ZERO, context);
    this.errorCode = errorCode;
  }

  public ErrorMap.ErrorCode errorCode() {
    return errorCode;
  }

  @Override
  public String description() {
    return "KeyValue error code decoded through Error Map. Error Code: " + errorCode;
  }

}
