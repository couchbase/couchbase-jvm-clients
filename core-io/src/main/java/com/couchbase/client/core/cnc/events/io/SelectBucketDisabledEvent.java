/*
 * Copyright (c) 2018 Couchbase, Inc.
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
 * This event is triggered when during the HELLO negotiation it was
 * determined that the server does not support the select bucket
 * command.
 *
 * <p>This is usually the case with servers that do not support
 * RBAC (role based access control) - so pre 5.0.</p>
 *
 * @since 2.0.0
 */
public class SelectBucketDisabledEvent extends AbstractEvent {

  private final String bucket;

  public SelectBucketDisabledEvent(final IoContext context, final String bucket) {
    super(Severity.DEBUG, Category.IO, Duration.ZERO, context);
    this.bucket = bucket;
  }

  /**
   * Returns the bucket name for this event.
   *
   * @return the bucket name.
   */
  public String bucket() {
    return bucket;
  }

  @Override
  public String description() {
    return "Select Bucket disabled/not negotiated during HELLO for bucket \"" + bucket + "\"";
  }
}
