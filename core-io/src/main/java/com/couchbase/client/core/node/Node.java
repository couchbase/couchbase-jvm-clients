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

package com.couchbase.client.core.node;

import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;

public class Node {

  /**
   * Instruct this {@link Node} to connect.
   *
   * <p>This method is async and will return immediately. Use the other methods available to
   * inspect the current state of the node, signaling potential successful connection
   * attempts.</p>
   */
  public void connect() {

  }

  /**
   * Instruct this {@link Node} to disconnect.
   *
   * <p>This method is async and will return immediately. Use the other methods available to
   * inspect the current state of the node, signaling potential successful disconnection
   * attempts.</p>
   */
  public void disconnect() {

  }

  /**
   * Sends the request into this {@link Node}.
   *
   * <p>Note that there is no guarantee that the request will actually dispatched, based on the
   * state this node is in.</p>
   *
   * @param request the request to send.
   */
  public <R extends Request<? extends Response>> void send(final R request) {

  }

  public NetworkAddress address() {
    return null;
  }
}
