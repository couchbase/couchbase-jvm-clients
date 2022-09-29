/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.transaction.components;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;

/**
 * This doesn't correspond to an individual server request.  It just makes it easier to slot into some existing
 * components, such as the ThresholdLoggingTracer, if we model a transaction as a BaseRequest.
 */
public class CoreTransactionRequest extends BaseRequest<CoreTransactionResponse> {

  public CoreTransactionRequest(Duration timeout, CoreContext ctx, RequestSpan span) {
    super(timeout, ctx, BestEffortRetryStrategy.INSTANCE, span);
  }

  @Override
  public ServiceType serviceType() {
    // This is part of the transactions 'virtual service'.  We don't want to add ServiceType.TRANSACTIONS
    // as that's a very wide-reaching change (e.g. users could waitUntilReady on it).
    return null;
  }

  @Override
  public String name() {
    return "transaction";
  }
}
