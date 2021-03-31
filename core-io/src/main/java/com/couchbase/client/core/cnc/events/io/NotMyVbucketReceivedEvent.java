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

import java.time.Duration;

/**
 * This event is raised if a KV response with "not my vbucket" is received.
 */
public class NotMyVbucketReceivedEvent extends AbstractEvent {

  private final short partition;

  public NotMyVbucketReceivedEvent(IoContext context, short partition) {
    super(Severity.DEBUG, Category.IO, Duration.ZERO, context);
    this.partition = partition;
  }

  @Override
  public String description() {
    return "Received a NotMyVBucket response from the server for partition " + partition + ", will retry.";
  }

}
