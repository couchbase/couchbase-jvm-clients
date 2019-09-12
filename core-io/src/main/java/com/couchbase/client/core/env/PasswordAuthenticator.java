/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.env;

import com.couchbase.client.core.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderNames;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpRequest;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.io.netty.kv.SaslAuthenticationHandler;
import com.couchbase.client.core.service.ServiceType;

import java.util.Base64;
import java.util.function.Supplier;

import static com.couchbase.client.core.util.Validators.notNull;
import static java.nio.charset.StandardCharsets.UTF_8;

public class PasswordAuthenticator implements Authenticator {

  private final Supplier<String> username;
  private final Supplier<String> password;

  public static PasswordAuthenticator create(String username, String password) {
    return PasswordAuthenticator.fromSupplier(() -> username, () -> password);
  }

  public static PasswordAuthenticator fromSupplier(Supplier<String> username, Supplier<String> password) {
    return new PasswordAuthenticator(username, password);
  }

  private PasswordAuthenticator(Supplier<String> username, Supplier<String> password) {
    notNull(username, "Username");
    notNull(password, "Password");

    this.username = username;
    this.password = password;
  }

  @Override
  public void authKeyValueConnection(final EndpointContext ctx, final ChannelPipeline pipeline) {
    pipeline.addLast(new SaslAuthenticationHandler(
      ctx,
      username.get(),
      password.get()
    ));
  }

  @Override
  public void authHttpRequest(final ServiceType serviceType, final HttpRequest request) {
    final String password = this.password.get();
    final String username = this.username.get();

    final String pw = password == null ? "" : password;
    final String encoded = Base64.getEncoder().encodeToString((username + ":" + pw).getBytes(UTF_8));
    request.headers().add(HttpHeaderNames.AUTHORIZATION, "Basic " + encoded);
  }

}
