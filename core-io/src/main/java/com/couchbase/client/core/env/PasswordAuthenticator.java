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

import com.couchbase.client.core.deps.io.grpc.Metadata;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderNames;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpRequest;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.io.netty.kv.SaslAuthenticationHandler;
import com.couchbase.client.core.io.netty.kv.SaslListMechanismsHandler;
import com.couchbase.client.core.service.ServiceType;

import java.util.Base64;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.Supplier;

import static com.couchbase.client.core.deps.io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Performs authentication against a couchbase server cluster using username and password.
 */
public class PasswordAuthenticator implements Authenticator {

  private static final Set<SaslMechanism> DEFAULT_SASL_MECHANISMS =
    EnumSet.of(SaslMechanism.SCRAM_SHA512, SaslMechanism.SCRAM_SHA256, SaslMechanism.SCRAM_SHA1);

  private final Supplier<String> username;
  private final Supplier<String> password;
  private final Set<SaslMechanism> allowedSaslMechanisms;
  private final String cachedHttpAuthHeader;

  /**
   * Creates a new {@link Builder} which allows to customize this authenticator.
   */
  public static PasswordAuthenticator.Builder builder() {
    return new Builder();
  }

  /**
   * Creates a new password authenticator with the default settings.
   *
   * @param username the username to use for all authentication.
   * @param password the password to use alongside the username.
   * @return the instantiated {@link PasswordAuthenticator}.
   */
  public static PasswordAuthenticator create(final String username, final String password) {
    return builder().username(username).password(password).build();
  }

  /**
   * Creates a LDAP compatible password authenticator which is INSECURE if not used with TLS.
   * <p>
   * Please note that this is INSECURE and will leak user credentials on the wire to eavesdroppers. This should
   * only be enabled in trusted environments.
   *
   * @param username the username to use for all authentication.
   * @param password the password to use alongside the username.
   * @return the instantiated {@link PasswordAuthenticator}.
   */
  public static PasswordAuthenticator ldapCompatible(final String username, final String password) {
    return builder().username(username).password(password).onlyEnablePlainSaslMechanism().build();
  }

  private PasswordAuthenticator(final Builder builder) {
    this.username = notNull(builder.username, "username");
    this.password = notNull(builder.password, "password");
    this.allowedSaslMechanisms = notNull(builder.allowedSaslMechanisms, "allowedSaslMechanisms");

    // We only pre-compute the http header if we get static credentials
    if (username instanceof OwnedSupplier && password instanceof OwnedSupplier) {
      cachedHttpAuthHeader = encodeAuthHttpHeader();
    } else {
      cachedHttpAuthHeader = null;
    }
  }

  /**
   * Helper method to encode the authentication http header value from the username and password.
   */
  private String encodeAuthHttpHeader() {
    final String password = this.password.get();
    final String username = this.username.get();

    final String pw = password == null ? "" : password;
    final String encoded = Base64.getEncoder().encodeToString((username + ":" + pw).getBytes(UTF_8));
    return "Basic " + encoded;
  }

  @Override
  public void authKeyValueConnection(final EndpointContext ctx, final ChannelPipeline pipeline) {
    boolean tls = ctx.environment().securityConfig().tlsEnabled();
    pipeline.addLast(new SaslListMechanismsHandler(ctx));
    pipeline.addLast(new SaslAuthenticationHandler(
      ctx,
      username.get(),
      password.get(),
      tls ? EnumSet.of(SaslMechanism.PLAIN) : allowedSaslMechanisms
    ));
  }

  @Override
  public void authHttpRequest(final ServiceType serviceType, final HttpRequest request) {
    request.headers().add(
      HttpHeaderNames.AUTHORIZATION,
      cachedHttpAuthHeader != null ? cachedHttpAuthHeader : encodeAuthHttpHeader()
    );
  }

  @Override
  public void authProtostellarRequest(Metadata metadata) {
    metadata.put(Metadata.Key.of("Authorization", ASCII_STRING_MARSHALLER), encodeAuthHttpHeader());
  }

  /**
   * Provides customization to the {@link PasswordAuthenticator}.
   */
  public static class Builder {

    private Supplier<String> username;
    private Supplier<String> password;
    private Set<SaslMechanism> allowedSaslMechanisms = DEFAULT_SASL_MECHANISMS;

    /**
     * Specifies a static username that will be used for all authentication purposes.
     *
     * @param username the username to use.
     * @return this builder for chaining purposes.
     */
    public Builder username(final String username) {
      notNullOrEmpty(username, "Username");
      return username(new OwnedSupplier<>(username));
    }

    /**
     * Specifies a dynamic username that will be used for all authentication purposes.
     * <p>
     * Every time the SDK needs to authenticate against the server, it will re-evaluate the supplier. This means that
     * you can pass in a supplier that dynamically loads a username from a (remote) source without taking the application
     * down on a restart.
     * <p>
     * It is VERY IMPORTANT that this supplier must not block on IO. It is called in async contexts and blocking for
     * a longer amount of time will stall SDK resources like async event loops.
     *
     * @param username the username to use.
     * @return this builder for chaining purposes.
     */
    public Builder username(final Supplier<String> username) {
      notNull(username, "Username");
      this.username = username;
      return this;
    }

    /**
     * Specifies a static password that will be used for all authentication purposes.
     *
     * @param password the password to alongside for the username provided.
     * @return this builder for chaining purposes.
     */
    public Builder password(final String password) {
      notNullOrEmpty(password, "Password");
      return password(new OwnedSupplier<>(password));
    }

    /**
     * Specifies a dynamic password that will be used for all authentication purposes.
     * <p>
     * Every time the SDK needs to authenticate against the server, it will re-evaluate the supplier. This means that
     * you can pass in a supplier that dynamically loads a password from a (remote) source without taking the application
     * down on a restart.
     * <p>
     * It is VERY IMPORTANT that this supplier must not block on IO. It is called in async contexts and blocking for
     * a longer amount of time will stall SDK resources like async event loops.
     *
     * @param password the password to alongside for the username provided.
     * @return this builder for chaining purposes.
     */
    public Builder password(final Supplier<String> password) {
      notNull(password, "Password");
      this.password = password;
      return this;
    }

    /**
     * Allows to set a list of allowed SASL mechanisms for the NON-TLS connections.
     * <p>
     * Note that if you add {@link SaslMechanism#PLAIN} to the list, this will cause credential leakage on the network
     * since PLAIN sends the credentials in cleartext. It is disabled by default to prevent downgrade attacks. We
     * recommend using a TLS connection instead.
     *
     * @param allowedSaslMechanisms the list of allowed sasl mechs for non-tls connections.
     * @return this builder for chaining purposes.
     */
    public Builder allowedSaslMechanisms(final Set<SaslMechanism> allowedSaslMechanisms) {
      notNullOrEmpty(allowedSaslMechanisms, "AllowedSaslMechanisms");
      this.allowedSaslMechanisms = allowedSaslMechanisms;
      return this;
    }

    /**
     * This method acts as a shortcut to {@link #allowedSaslMechanisms(Set)} which adds {@link SaslMechanism#PLAIN}
     * to the allowed mechanism list for NON TLS connections.
     * <p>
     * Please note that this is INSECURE and will leak user credentials on the wire to eavesdroppers. This should
     * only be enabled in trusted environments - we recommend connecting via TLS to the cluster instead.
     * <p>
     * If you are running an LDAP enabled environment, please use {@link #onlyEnablePlainSaslMechanism()} instead!
     *
     * @return this builder for chaining purposes.
     */
    public Builder enablePlainSaslMechanism() {
      return allowedSaslMechanisms(EnumSet.allOf(SaslMechanism.class));
    }

    /**
     * This method will ONLY enable the PLAIN SASL mechanism (useful for LDAP enabled environments).
     * <p>
     * Please note that this is INSECURE and will leak user credentials on the wire to eavesdroppers. This should
     * only be enabled in trusted environments - we recommend connecting via TLS to the cluster instead.
     * <p>
     * You might also want to consider using the static constructor method {@link #ldapCompatible(String, String)} as a
     * shortcut.
     *
     * @return this builder for chaining purposes.
     */
    public Builder onlyEnablePlainSaslMechanism() {
      return allowedSaslMechanisms(EnumSet.of(SaslMechanism.PLAIN));
    }

    /**
     * Creates the {@link PasswordAuthenticator} based on the customization in this builder.
     *
     * @return the created password authenticator instance.
     */
    public PasswordAuthenticator build() {
      return new PasswordAuthenticator(this);
    }
  }

}
