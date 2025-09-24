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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.io.netty.kv.SaslAuthenticationHandler;
import com.couchbase.client.core.io.netty.kv.SaslListMechanismsHandler;
import com.couchbase.client.core.io.netty.kv.sasl.SaslHelper;
import reactor.util.annotation.Nullable;

import java.util.Base64;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.Supplier;

import static com.couchbase.client.core.io.netty.kv.sasl.SaslHelper.platformHasSaslPlain;
import static com.couchbase.client.core.util.CbCollections.setCopyOf;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

/**
 * Performs authentication against a Couchbase server cluster using username and password.
 */
public class PasswordAuthenticator implements Authenticator {

  private static final Set<SaslMechanism> DEFAULT_SASL_MECHANISMS =
    unmodifiableSet(EnumSet.of(
      SaslMechanism.SCRAM_SHA512,
      SaslMechanism.SCRAM_SHA256,
      SaslMechanism.SCRAM_SHA1
    ));

  private final Supplier<UsernameAndPassword> usernameAndPassword;
  private final Set<SaslMechanism> allowedSaslMechanisms;
  private final @Nullable String cachedHttpAuthHeader;

  /**
   * Creates a new {@link Builder} which allows to customize this authenticator.
   *
   * @see #builder(String, String)
   * @see #builder(Supplier)
   * @deprecated Please use one of the "See Also" methods instead,
   * to ensure all required builder properties are set.
   */
  @Deprecated
  public static PasswordAuthenticator.Builder builder() {
    return new Builder();
  }

  /**
   * Creates a builder for an authenticator that uses the given credentials.
   */
  public static PasswordAuthenticator.Builder builder(String username, String password) {
    return new Builder(username, password);
  }

  /**
   * Creates a builder for an authenticator that uses dynamic credentials.
   * This enables updating credentials without having to restart your application.
   * <p>
   * <b>IMPORTANT:</b> The supplier's {@code get()} method must not do blocking IO.
   * It is called from async contexts, where blocking IO can starve important SDK resources
   * like async event loops.
   * <p>
   * Instead of blocking inside the supplier, the supplier should return a value
   * from a volatile field.
   * <p>
   * One way to keep the supplier's value up to date is to schedule a recurring task
   * that reads new credentials from an external source and stores them
   * in the volatile field.
   *
   * @deprecated This method is difficult to use safely, because it's easy to accidentally
   * do blocking IO inside the supplier. Please use {@code cluster.setAuthenticator(newAuthenticator)} instead.
   */
  @Deprecated
  public static PasswordAuthenticator.Builder builder(Supplier<UsernameAndPassword> supplier) {
    return new Builder(supplier);
  }

  /**
   * Creates a new password authenticator with the default settings.
   *
   * @param username the username to use for all authentication.
   * @param password the password to use alongside the username.
   * @return the instantiated {@link PasswordAuthenticator}.
   * @see #builder(String, String)
   * @see #builder(Supplier)
   */
  public static PasswordAuthenticator create(final String username, final String password) {
    return builder(username, password).build();
  }

  /**
   * Creates an LDAP compatible password authenticator which is INSECURE if not used with TLS.
   * <p>
   * Please note that this is INSECURE and will leak user credentials on the wire to eavesdroppers. This should
   * only be enabled in trusted environments.
   *
   * @param username the username to use for all authentication.
   * @param password the password to use alongside the username.
   * @return the instantiated {@link PasswordAuthenticator}.
   */
  public static PasswordAuthenticator ldapCompatible(final String username, final String password) {
    return builder(username, password).onlyEnablePlainSaslMechanism().build();
  }

  private PasswordAuthenticator(final Builder builder) {
    EnumSet<SaslMechanism> tempMechanisms = EnumSet.noneOf(SaslMechanism.class);
    tempMechanisms.addAll(builder.allowedSaslMechanisms);
    this.allowedSaslMechanisms = unmodifiableSet(tempMechanisms);

    this.usernameAndPassword = builder.resolveUsernameAndPasswordSupplier();

    // Only pre-compute the HTTP header if we get static credentials
    cachedHttpAuthHeader = builder.dynamicCredentials ? null : encodeAuthHttpHeader(this.usernameAndPassword.get());
  }

  @Override
  public String getAuthHeaderValue() {
    return cachedHttpAuthHeader != null
      ? cachedHttpAuthHeader
      : encodeAuthHttpHeader(this.usernameAndPassword.get());
  }

  private static String encodeAuthHttpHeader(UsernameAndPassword credentials) {
    byte[] encodeMe = (credentials.username() + ":" + credentials.password()).getBytes(UTF_8);
    String encoded = Base64.getEncoder().encodeToString(encodeMe);
    return "Basic " + encoded;
  }

  @Override
  public void authKeyValueConnection(final EndpointContext ctx, final ChannelPipeline pipeline) {
    boolean tls = ctx.environment().securityConfig().tlsEnabled();
    boolean forceSaslPlain = tls && platformHasSaslPlain();
    UsernameAndPassword credentials = usernameAndPassword.get();

    pipeline.addLast(new SaslListMechanismsHandler(ctx));
    pipeline.addLast(new SaslAuthenticationHandler(
      ctx,
      credentials.username(),
      credentials.password(),
      forceSaslPlain ? EnumSet.of(SaslMechanism.PLAIN) : allowedSaslMechanisms
    ));
  }

  @Override
  public boolean requiresTls() {
    return false;
  }

  /**
   * Provides customization to the {@link PasswordAuthenticator}.
   */
  public static class Builder {

    private Supplier<String> username;
    private Supplier<String> password;
    private Set<SaslMechanism> allowedSaslMechanisms = DEFAULT_SASL_MECHANISMS;
    private Supplier<Boolean> platformHasSaslPlain = SaslHelper::platformHasSaslPlain;
    private Supplier<UsernameAndPassword> usernameAndPassword;

    /**
     * True if at least one property was specified using a supplier instead of a fixed value.
     */
    private boolean dynamicCredentials;

    /**
     * @see PasswordAuthenticator#builder(Supplier)
     */
    private Builder(Supplier<UsernameAndPassword> usernameAndPassword) {
      this.usernameAndPassword = requireNonNull(usernameAndPassword);
      this.dynamicCredentials = true;
    }

    /**
     * @see PasswordAuthenticator#builder(String, String)
     */
    private Builder(String username, String password) {
      UsernameAndPassword cached = new UsernameAndPassword(username, password);
      this.usernameAndPassword = () -> cached;
      this.dynamicCredentials = false;
    }

    /**
     * @deprecated Please use {@link Builder(String, String)} or {@link Builder(Supplier)} instead.
     */
    @Deprecated
    public Builder() {
    }

    /**
     * Specifies a static username that will be used for all authentication purposes.
     *
     * @param username the username to use.
     * @return this builder for chaining purposes.
     * @see PasswordAuthenticator#builder(String, String)
     * @see PasswordAuthenticator#builder(Supplier)
     * @deprecated Please specify the username when creating the builder,
     * using one of the "See Also" methods.
     */
    @Deprecated
    public Builder username(final String username) {
      requireDeprecatedConstructor();
      notNullOrEmpty(username, "Username");
      this.username = () -> username;
      return this;
    }

    /**
     * Specifies a dynamic username that will be used for all authentication purposes.
     * This enables updating credentials without having to restart your application.
     * <p>
     * <b>IMPORTANT:</b> The supplier's {@code get()} method must not do blocking IO.
     * See {@link PasswordAuthenticator#builder(Supplier)} for details about why this is
     * important, and what to do instead of blocking IO.
     *
     * @param username A supplier that returns the username to use.
     * @return this builder for chaining purposes.
     * @deprecated This method does not support returning username and password as an atomic unit.
     * Please use {@code cluster.setAuthenticator(newAuthenticator)} instead.
     */
    @Deprecated
    public Builder username(final Supplier<String> username) {
      requireDeprecatedConstructor();
      notNull(username, "Username");
      this.username = username;
      this.dynamicCredentials = true;
      return this;
    }

    /**
     * Specifies a static password that will be used for all authentication purposes.
     *
     * @param password the password to alongside for the username provided.
     * @return this builder for chaining purposes.
     * @see PasswordAuthenticator#builder(String, String)
     * @see PasswordAuthenticator#builder(Supplier)
     * @deprecated Please specify the password when creating the builder,
     * using one of the "See Also" methods.
     */
    @Deprecated
    public Builder password(final String password) {
      requireDeprecatedConstructor();
      notNullOrEmpty(password, "Password");
      this.password = () -> password;
      return this;
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
     * @deprecated This method does not support returning username and password as an atomic unit.
     * Please use {@code cluster.setAuthenticator(newAuthenticator)} instead.
     */
    @Deprecated
    public Builder password(final Supplier<String> password) {
      requireDeprecatedConstructor();
      notNull(password, "Password");
      this.password = password;
      this.dynamicCredentials = true;
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

      // Fail fast if LDAP-compatible auth won't work because platform does not have SASL PLAIN
      if (allowedSaslMechanisms.equals(setOf(SaslMechanism.PLAIN)) && !platformHasSaslPlain.get()) {
        throw new RuntimeException(
          "This JVM is running in a restricted mode that prevents using SASL PLAIN for authentication."
        );
      }

      this.allowedSaslMechanisms = setCopyOf(allowedSaslMechanisms);
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

    // Visible for testing
    @Stability.Internal
    Builder setPlatformHasSaslPlain(Supplier<Boolean> saslPlainAvailable) {
      this.platformHasSaslPlain = requireNonNull(saslPlainAvailable);
      return this;
    }

    /**
     * Creates the {@link PasswordAuthenticator} based on the customization in this builder.
     *
     * @return the created password authenticator instance.
     */
    public PasswordAuthenticator build() {
      return new PasswordAuthenticator(this);
    }

    private void requireDeprecatedConstructor() {
      if (usernameAndPassword != null) {
        throw new IllegalStateException("Username and password were specified when this builder was created, and cannot be changed.");
      }
    }

    private Supplier<UsernameAndPassword> resolveUsernameAndPasswordSupplier() {
      // This is where we'll end up if the user does not call any
      // deprecated methods.
      if (usernameAndPassword != null) {
        return usernameAndPassword;
      }

      // Merge the separate username and password suppliers
      // into a single Supplier<UsernameAndPassword>. Ugh.

      notNull(username, "Must specify username");
      notNull(password, "Must specify password");

      if (!dynamicCredentials) {
        // Eagerly get username and password from the suppliers,
        // and use them to build a cached instance.
        UsernameAndPassword cached = new UsernameAndPassword(
          username.get(),
          password.get()
        );
        return () -> cached;
      }

      // Username or password or both were specified using
      // deprecated methods and independent suppliers.
      // Stitch the suppliers together into a composite supplier.
      //
      // NOTE: This doesn't fix the problem of the username and password
      // suppliers not being atomic, but there's no way to fix that.
      return () -> new UsernameAndPassword(
        username.get(),
        password.get()
      );
    }
  }

  @Override
  public String toString() {
    // Omit sensitive information like password or auth header.
    return "PasswordAuthenticator{" +
      "allowedSaslMechanisms=" + allowedSaslMechanisms +
      '}';
  }
}
