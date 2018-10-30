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

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.Response;

import java.time.Duration;

/**
 * The {@link BaseKeyValueRequest} should be subclassed by all KeyValue requests since it
 * provides common ground for all of them (i.e. adding the kv partition needed).
 *
 * @since 2.0.0
 */
public abstract class BaseKeyValueRequest<RES extends Response>
  extends BaseRequest<RES>
  implements KeyValueRequest<RES> {

  /**
   * Once set, stores the partition where this request should be dispatched against.
   */
  private volatile short partition;

  BaseKeyValueRequest(final Duration timeout, final RequestContext ctx) {
    super(timeout, ctx);
  }

  @Override
  public short partition() {
    return partition;
  }

  @Override
  public void partition(short partition) {
    this.partition = partition;
  }

}
