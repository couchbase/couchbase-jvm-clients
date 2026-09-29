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

import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.msg.ResponseStatus;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Optional;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * Helper methods that need to be used when dealing with the HTTP protocol.
 *
 * @since 2.0.0
 */
public class HttpProtocol {

  /**
   * Converts the http protocol status into its generic format.
   *
   * @param status the protocol status.
   * @return the response status.
   */
  public static ResponseStatus decodeStatus(final HttpResponseStatus status) {
    return status == null ? ResponseStatus.UNKNOWN : decodeStatus(status.code());
  }

  public static ResponseStatus decodeStatus(final int status) {
    if (status == HttpResponseStatus.OK.code()
      || status == HttpResponseStatus.ACCEPTED.code()
      || status == HttpResponseStatus.CREATED.code()) {
      return ResponseStatus.SUCCESS;
    } else if (status == HttpResponseStatus.NOT_FOUND.code()) {
      return ResponseStatus.NOT_FOUND;
    } else if (status == HttpResponseStatus.BAD_REQUEST.code()) {
      return ResponseStatus.INVALID_ARGS;
    } else if (status == HttpResponseStatus.INTERNAL_SERVER_ERROR.code()) {
      return ResponseStatus.INTERNAL_SERVER_ERROR;
    } else if (status == HttpResponseStatus.UNAUTHORIZED.code()
      || status == HttpResponseStatus.FORBIDDEN.code()) {
      return ResponseStatus.NO_ACCESS;
    } else if (status == HttpResponseStatus.TOO_MANY_REQUESTS.code()) {
      return ResponseStatus.TOO_MANY_REQUESTS;
    } else {
      return ResponseStatus.UNKNOWN;
    }
  }

}
