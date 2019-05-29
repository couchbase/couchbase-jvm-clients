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

package com.couchbase.client.core.io.netty;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.handler.codec.base64.Base64;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderNames;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpRequest;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Helper methods that need to be used when dealing with the HTTP protocol.
 *
 * @since 2.0.0
 */
public class HttpProtocol {

  /**
   * Adds http basic auth to a given request.
   *
   * @param request the request where it should be added.
   * @param user the username.
   * @param password the password.
   */
  public static void addHttpBasicAuth(final HttpRequest request, final String user,
                                      final String password) {
    // if both user and password are null or empty, don't add http basic auth
    // this is usually the case when certificate auth is used.
    if ((user == null || user.isEmpty()) && (password == null || password.isEmpty())) {
      return;
    }

    final String pw = password == null ? "" : password;

    ByteBuf raw = Unpooled.buffer(user.length() + pw.length() + 1);
    raw.writeBytes((user + ":" + pw).getBytes(UTF_8));
    ByteBuf encoded = Base64.encode(raw, false);
    request.headers().add(HttpHeaderNames.AUTHORIZATION, "Basic "
      + encoded.toString(UTF_8));
    encoded.release();
    raw.release();
  }

  /**
   * Calculates the remote host for caching so that it is set on each query request.
   *
   * @param remoteAddress the remote address.
   * @return the converted remote http host.
   */
  public static String remoteHttpHost(final SocketAddress remoteAddress) {
    final String remoteHost;
    if (remoteAddress instanceof InetSocketAddress) {
      InetSocketAddress inetAddr = (InetSocketAddress) remoteAddress;
      remoteHost = inetAddr.getAddress().getHostAddress() + ":" + inetAddr.getPort();
    } else {
      remoteHost = remoteAddress.toString();
    }
    return remoteHost;
  }

}
