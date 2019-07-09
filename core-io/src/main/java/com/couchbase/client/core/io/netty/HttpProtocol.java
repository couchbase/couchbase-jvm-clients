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

import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderNames;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.env.Credentials;
import com.couchbase.client.core.msg.ResponseStatus;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Base64;

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
   * @param credentials the credentials to use.
   */
  public static void addHttpBasicAuth(final HttpRequest request, final Credentials credentials) {
    final String user = credentials.username();
    final String password = credentials.password();

    // if both user and password are null or empty, don't add http basic auth
    // this is usually the case when certificate auth is used.
    if ((user == null || user.isEmpty()) && (password == null || password.isEmpty())) {
      return;
    }

    final String pw = password == null ? "" : password;
    final String encoded = Base64.getEncoder().encodeToString((user + ":" + pw).getBytes(UTF_8));
    request.headers().add(HttpHeaderNames.AUTHORIZATION, "Basic " + encoded);
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

  /**
   * Converts the http protocol status into its generic format.
   *
   * @param status the protocol status.
   * @return the response status.
   */
  public static ResponseStatus decodeStatus(final HttpResponseStatus status) {
    if (status == null) {
      return ResponseStatus.UNKNOWN;
    }

    if (status.equals(HttpResponseStatus.OK)
      || status.equals(HttpResponseStatus.ACCEPTED)
      || status.equals(HttpResponseStatus.CREATED)) {
      return ResponseStatus.SUCCESS;
    } else if (status.equals(HttpResponseStatus.NOT_FOUND)) {
      return ResponseStatus.NOT_FOUND;
    } else if (status.equals(HttpResponseStatus.BAD_REQUEST)) {
      return ResponseStatus.INVALID_ARGS;
    } else if (status.equals(HttpResponseStatus.INTERNAL_SERVER_ERROR)) {
      return ResponseStatus.INTERNAL_ERROR;
    } else if (status.equals(HttpResponseStatus.UNAUTHORIZED)) {
      return ResponseStatus.NO_ACCESS;
    } else if (status.equals(HttpResponseStatus.TOO_MANY_REQUESTS)) {
      return ResponseStatus.TOO_MANY_REQUESTS;
    } else {
      return ResponseStatus.UNKNOWN;
    }
  }

}
