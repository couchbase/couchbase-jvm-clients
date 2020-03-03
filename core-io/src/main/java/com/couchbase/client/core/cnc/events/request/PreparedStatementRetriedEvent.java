/*
 * Copyright (c) 2020 Couchbase, Inc.
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

package com.couchbase.client.core.cnc.events.request;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.retry.RetryReason;

import java.time.Duration;

public class PreparedStatementRetriedEvent extends AbstractEvent {

  private final RetryReason retryReason;
  private final Throwable cause;

  public PreparedStatementRetriedEvent(Duration duration, RequestContext context, RetryReason retryReason, Throwable cause) {
    super(Severity.DEBUG, Category.REQUEST, duration, context);
    this.retryReason = retryReason;
    this.cause = cause;
  }

  @Override
  public Throwable cause() {
    return cause;
  }

  @Override
  public String description() {
    return "Prepared Statement request retry scheduled per RetryStrategy (Reason: " + retryReason + ")";
  }

}
