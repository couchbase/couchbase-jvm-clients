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

package com.couchbase.client.core.io;

import com.couchbase.client.core.CoreContext;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Optional;

/**
 * The {@link IoContext} is used to extend the core context with IO related metadata
 * that is useful during event generation.
 *
 * @since 2.0.0
 */
public class IoContext extends CoreContext {

  private final SocketAddress localSocket;

  private final SocketAddress remoteSocket;

  private final Optional<String> bucket;

  /**
   * Creates a new IO Context.
   *
   * @param ctx the core context as a parent.
   * @param localSocket the local io socket.
   * @param remoteSocket the remote io socket.
   * @param bucket the bucket name, if it makes sense.
   */
  public IoContext(final CoreContext ctx, final SocketAddress localSocket,
                   final SocketAddress remoteSocket, final Optional<String> bucket) {
    super(ctx.core(), ctx.id(), ctx.environment());
    this.localSocket = localSocket;
    this.remoteSocket = remoteSocket;
    this.bucket = bucket;
  }

  @Override
  protected void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);
    input.put("local", localSocket());
    input.put("remote", remoteSocket());
    bucket.ifPresent(s -> input.put("bucket", s));
  }

  /**
   * Returns the local socket.
   */
  public SocketAddress localSocket() {
    return localSocket;
  }

  /**
   * Returns the remote socket.
   */
  public SocketAddress remoteSocket() {
    return remoteSocket;
  }

  /**
   * Returns the bucket name if present.
   */
  public Optional<String> bucket() {
    return bucket;
  }
}