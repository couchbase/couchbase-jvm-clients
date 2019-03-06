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
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;

public class HttpProtocol {

  public static void addHttpBasicAuth(final HttpRequest request, final String user,
                                      final String password) {
    // if both user and password are null or empty, don't add http basic auth
    // this is usually the case when certificate auth is used.
    if ((user == null || user.isEmpty()) && (password == null || password.isEmpty())) {
      return;
    }

    final String pw = password == null ? "" : password;

    ByteBuf raw = Unpooled.buffer(user.length() + pw.length() + 1);
    raw.writeBytes((user + ":" + pw).getBytes(CharsetUtil.UTF_8));
    ByteBuf encoded = Base64.encode(raw, false);
    request.headers().add(HttpHeaderNames.AUTHORIZATION, "Basic "
      + encoded.toString(CharsetUtil.UTF_8));
    encoded.release();
    raw.release();
  }

}
