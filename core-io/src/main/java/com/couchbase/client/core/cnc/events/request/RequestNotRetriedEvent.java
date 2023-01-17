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

package com.couchbase.client.core.cnc.events.request;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Context;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.retry.RetryReason;

import java.time.Duration;

/**
 * This event is raised if a request is not retried anymore.
 */
public class RequestNotRetriedEvent extends AbstractEvent {

  private final Class<?> clazz;
  private final RetryReason retryReason;
  private final Throwable throwable;

  public RequestNotRetriedEvent(final Severity severity, final Class<?> clazz, final Context context,
                                final RetryReason reason, final Throwable throwable) {
    super(severity, Category.REQUEST, Duration.ZERO, context);
    this.clazz = clazz;
    this.retryReason = reason;
    this.throwable = throwable;
  }

  public RetryReason retryReason() {
    return retryReason;
  }

  @Override
  public Throwable cause() {
    return throwable;
  }

  @Override
  public String description() {
    return "Request " + clazz.getSimpleName() + " not retried per RetryStrategy (Reason: " + retryReason + ")";
  }

}
