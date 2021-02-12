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

package com.couchbase.client.core.msg.manager;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;

/**
 * Base class for the manager requests, mainly to define the service type in a uniform way.
 */
public abstract class BaseManagerRequest<R extends Response> extends BaseRequest<R> implements ManagerRequest<R> {

  BaseManagerRequest(final Duration timeout, final CoreContext ctx, final RetryStrategy retryStrategy) {
    super(timeout, ctx, retryStrategy);
  }

  BaseManagerRequest(final Duration timeout, final CoreContext ctx, final RetryStrategy retryStrategy, RequestSpan span) {
    super(timeout, ctx, retryStrategy, span);
  }

  @Override
  public ServiceType serviceType() {
    return ServiceType.MANAGER;
  }

}
