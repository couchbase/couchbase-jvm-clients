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
 * This event gets created as soon as a bucket has been selected during the
 * KV bootstrap process for each socket/node.
 *
 * @since 2.0.0
 */
public class SelectBucketCompletedEvent extends AbstractEvent {

  /**
   * The name of the bucket that got selected.
   */
  private final String bucket;

  public SelectBucketCompletedEvent(final Duration duration, final IoContext context,
                                    final String bucket) {
    super(Severity.DEBUG, Category.IO, duration, context);
    this.bucket = bucket;
  }

  /**
   * Returns the name of the bucket that got selected.
   */
  public String bucket() {
    return bucket;
  }

  @Override
  public String description() {
    return "Bucket \"" + bucket + "\" selected";
  }

}
