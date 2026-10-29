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

import com.couchbase.client.core.endpoint.http.CoreHttpStatus;
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
   * Converts the HTTP status code into its generic format.
   *
   * @param status the HTTP status code.
   * @return the response status.
   */
  public static ResponseStatus decodeStatus(final int status) {
    if (status == CoreHttpStatus.OK
      || status == CoreHttpStatus.ACCEPTED
      || status == CoreHttpStatus.CREATED) {
      return ResponseStatus.SUCCESS;
    } else if (status == CoreHttpStatus.NOT_FOUND) {
      return ResponseStatus.NOT_FOUND;
    } else if (status == CoreHttpStatus.BAD_REQUEST) {
      return ResponseStatus.INVALID_ARGS;
    } else if (status == CoreHttpStatus.INTERNAL_SERVER_ERROR) {
      return ResponseStatus.INTERNAL_SERVER_ERROR;
    } else if (status == CoreHttpStatus.UNAUTHORIZED
      || status == CoreHttpStatus.FORBIDDEN) {
      return ResponseStatus.NO_ACCESS;
    } else if (status == CoreHttpStatus.TOO_MANY_REQUESTS) {
      return ResponseStatus.TOO_MANY_REQUESTS;
    } else {
      return ResponseStatus.UNKNOWN;
    }
  }

}
