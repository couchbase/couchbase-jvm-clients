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

package com.couchbase.client.core.io.netty.search;

import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.io.netty.ChunkedHandlerSwitcher;
import com.couchbase.client.core.msg.search.ServerSearchRequest;

public class SearchHandlerSwitcher extends ChunkedHandlerSwitcher {

  public SearchHandlerSwitcher(final BaseEndpoint endpoint, final EndpointContext context) {
    super(
      new ChunkedSearchMessageHandler(endpoint, context),
      new NonChunkedSearchMessageHandler(endpoint),
      ServerSearchRequest.class
    );
  }

}
