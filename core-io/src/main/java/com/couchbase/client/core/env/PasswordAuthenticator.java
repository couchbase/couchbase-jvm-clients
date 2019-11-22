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
import java.util.EnumSet;
import java.util.Set;
import java.util.function.Supplier;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.nio.charset.StandardCharsets.UTF_8;

public class PasswordAuthenticator implements Authenticator {

  private static final Set<SaslMechanism> DEFAULT_SASL_MECHANISMS =
    EnumSet.of(SaslMechanism.SCRAM_SHA512, SaslMechanism.SCRAM_SHA256, SaslMechanism.SCRAM_SHA1,
      SaslMechanism.PLAIN // TODO: this will be disabled later, but more changes to mock needed
  );

  private final Supplier<String> username;
  private final Supplier<String> password;
  private final Set<SaslMechanism> allowedSaslMechanisms;

  public static PasswordAuthenticator.Builder builder() {
    return new Builder();
  }

  public static PasswordAuthenticator create(final String username, final String password) {
    return builder().username(() -> username).password(() -> password).build();
  }

  private PasswordAuthenticator(final Builder builder) {
    this.username = builder.username;
    this.password = builder.password;
    this.allowedSaslMechanisms = builder.allowedSaslMechanisms;
  }

  @Override
  public void authKeyValueConnection(final EndpointContext ctx, final ChannelPipeline pipeline) {
    boolean tls = ctx.environment().securityConfig().tlsEnabled();
    pipeline.addLast(new SaslAuthenticationHandler(
      ctx,
      username.get(),
      password.get(),
      tls ? EnumSet.allOf(SaslMechanism.class) : allowedSaslMechanisms
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

  public static class Builder {

    private Supplier<String> username;
    private Supplier<String> password;
    private Set<SaslMechanism> allowedSaslMechanisms = DEFAULT_SASL_MECHANISMS;

    public Builder username(Supplier<String> username) {
      notNull(username, "Username");
      this.username = username;
      return this;
    }

    public Builder password(Supplier<String> password) {
      notNull(password, "Password");
      this.password = password;
      return this;
    }

    /**
     * Allows to set a list of allowed SASL mechanisms for the NON-TLS connections.
     *
     * @param allowedSaslMechanisms the list of allowed sasl mechs for non-tls connections
     */
    public Builder allowedSaslMechanisms(Set<SaslMechanism> allowedSaslMechanisms) {
      notNullOrEmpty(allowedSaslMechanisms, "AllowedSaslMechanisms");
      this.allowedSaslMechanisms = allowedSaslMechanisms;
      return this;
    }

    public PasswordAuthenticator build() {
      return new PasswordAuthenticator(this);
    }
  }

}