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
    return "Node list and configuration's partition hosts sizes are not equal. " +
      actualSize + " vs. " + configSize;
  }
}
