/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.io.netty.kv.sasl;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.env.SaslMechanism;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.EnumSet;
import java.util.Set;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

/**
 * What a KV connection authenticates with, using SASL: the same values an authenticator passes to
 * {@code SaslAuthenticationHandler}, for KV connections that don't use Netty.
 */
@NullMarked
@Stability.Internal
public final class SaslCredentials {
  private final @Nullable String username;
  private final String password;
  private final Set<SaslMechanism> allowedMechanisms;

  /**
   * @param username null if the mechanism doesn't need one (for example, OAUTHBEARER).
   * @param password the password, or the token.
   */
  public SaslCredentials(@Nullable String username, String password, Set<SaslMechanism> allowedMechanisms) {
    if (allowedMechanisms.isEmpty()) {
      throw new IllegalArgumentException("Must allow at least one SASL mechanism.");
    }
    this.username = username;
    this.password = requireNonNull(password);
    this.allowedMechanisms = unmodifiableSet(EnumSet.copyOf(allowedMechanisms));
  }

  public @Nullable String username() {
    return username;
  }

  public String password() {
    return password;
  }

  public Set<SaslMechanism> allowedMechanisms() {
    return allowedMechanisms;
  }

  @Override
  public String toString() {
    return "SaslCredentials{" +
      "username=" + redactUser(username) +
      ", password=<redacted>" + // MUST NOT appear in logs.
      ", allowedMechanisms=" + allowedMechanisms +
      '}';
  }
}
