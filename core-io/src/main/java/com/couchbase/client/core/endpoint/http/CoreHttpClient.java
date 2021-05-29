/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.endpoint.http;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.msg.RequestTarget;

import static com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.DELETE;
import static com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.GET;
import static com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.POST;
import static com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.PUT;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreHttpClient {
  private final Core core;
  private final RequestTarget target;

  public CoreHttpClient(Core core, RequestTarget target) {
    this.core = requireNonNull(core);
    this.target = requireNonNull(target);
  }

  public CoreHttpRequest.Builder get(CoreHttpPath path, CoreCommonOptions options) {
    return newRequest(GET, path, options);
  }

  public CoreHttpRequest.Builder put(CoreHttpPath path, CoreCommonOptions options) {
    return newRequest(PUT, path, options);
  }

  public CoreHttpRequest.Builder post(CoreHttpPath path, CoreCommonOptions options) {
    return newRequest(POST, path, options);
  }

  public CoreHttpRequest.Builder delete(CoreHttpPath path, CoreCommonOptions options) {
    return newRequest(DELETE, path, options);
  }

  public CoreHttpRequest.Builder newRequest(HttpMethod method, CoreHttpPath path, CoreCommonOptions options) {
    return new CoreHttpRequest.Builder(options, core.context(), target, method, path);
  }
}
