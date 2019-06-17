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

import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponse;
import com.couchbase.client.core.msg.Encodable;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;

/**
 * Parent interface for all requests going to the cluster manager.
 */
public interface ManagerRequest<R extends Response> extends Request<R>, Encodable<FullHttpRequest> {

  /**
   * Decodes a manager response into its response entity.
   *
   * @param response the http header of the response.
   * @param content the actual content of the response.
   * @return the decoded value.
   */
  R decode(HttpResponse response, byte[] content);

}
