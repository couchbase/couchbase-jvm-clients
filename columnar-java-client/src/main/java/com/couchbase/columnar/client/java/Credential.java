/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.columnar.client.java;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.columnar.client.java.internal.DynamicAuthenticator;
import com.couchbase.columnar.client.java.internal.ThreadSafe;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Create an instance like this:
 * <pre>
 * Credential.of(username, password)
 * </pre>
 * <p>
 * For advanced use cases involving dynamic credentials, see
 * {@link Credential#ofDynamic(Supplier)}.
 */
@ThreadSafe
public abstract class Credential {

  /**
   * Returns a new instance that holds the given username and password.
   */
  public static Credential of(String username, String password) {
    Authenticator authenticator = PasswordAuthenticator.create(username, password);
    return new Credential() {
      @Override
      Authenticator toInternalAuthenticator() {
        return authenticator;
      }
    };
  }

  /**
   * Returns a new instance of a dynamic credential that invokes the given supplier
   * the supplier every time a credential is required.
   * <p>
   * This enables updating a credential without having to restart your application.
   * <p>
   * <b>IMPORTANT:</b> The supplier's {@code get()} method must not do blocking IO
   * or other expensive operations. It is called from async contexts, where blocking
   * can starve important SDK resources like async event loops.
   * <p>
   * Instead of blocking inside the supplier, consider having the supplier return a value
   * from a volatile field. Then keep the field up to date by scheduling a recurring task
   * that reads new credentials from an external source and stores them
   * in the volatile field.
   */
  public static Credential ofDynamic(Supplier<Credential> supplier) {
    requireNonNull(supplier);
    Authenticator authenticator = new DynamicAuthenticator(() -> supplier.get().toInternalAuthenticator());
    return new Credential() {
      @Override
      Authenticator toInternalAuthenticator() {
        return authenticator;
      }
    };
  }

  @Stability.Internal
  abstract Authenticator toInternalAuthenticator();

  /**
   * @see #of
   * @see #ofDynamic
   */
  private Credential() {
  }
}
