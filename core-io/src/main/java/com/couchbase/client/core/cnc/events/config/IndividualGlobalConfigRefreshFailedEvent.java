/*
 * Copyright (c) 2022 Couchbase, Inc.
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

package com.couchbase.client.core.cnc.events.config;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.msg.kv.CarrierGlobalConfigResponse;

import java.time.Duration;

/**
 * Raised if an individual global config refresh event failed, but it is not raised as a warning since we expect
 * the next attempt(s) to complete.
 */
public class IndividualGlobalConfigRefreshFailedEvent extends AbstractEvent {

  private final Throwable cause;
  private final String host;

  private final CarrierGlobalConfigResponse response;

  public IndividualGlobalConfigRefreshFailedEvent(Duration duration, CoreContext context, Throwable cause, String host,
                                                  CarrierGlobalConfigResponse response) {
    super(Severity.DEBUG, Category.CONFIG, duration, context);
    this.cause = cause;
    this.host = host;
    this.response = response;
  }

  @Override
  public String description() {
    return "Refreshing a global config from node \"" + host + "\" failed, but will try the next host."
      + (response != null ? " Response: " + response : "");
  }

  @Override
  public Throwable cause() {
    return cause;
  }

}
