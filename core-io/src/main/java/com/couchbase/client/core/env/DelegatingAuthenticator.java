/*
 * Copyright 2025 Couchbase, Inc.
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

package com.couchbase.client.core.env;

import org.jspecify.annotations.NullMarked;

import static java.util.Objects.requireNonNull;

/**
 * Delegates authentication to another authenticator.
 * <p>
 * The other authenticator can be swapped at runtime
 * to support credential rotation.
 *
 * @see #create(Authenticator)
 */
@NullMarked
public class DelegatingAuthenticator extends AuthenticatorWrapper {
  private volatile Authenticator delegate;

  /**
   * Returns a new authenticator that delegates to the given authenticator.
   * <p>
   * The delegate may be updated later by calling {@link #setDelegate(Authenticator)}.
   */
  public static DelegatingAuthenticator create(Authenticator delegate) {
    return new DelegatingAuthenticator(delegate);
  }

  public void setDelegate(Authenticator delegate) {
    this.delegate = requireNonNull(delegate);
  }

  DelegatingAuthenticator(Authenticator delegate) {
    setDelegate(delegate);
  }

  protected Authenticator wrapped() {
    return delegate;
  }

  @Override
  public String toString() {
    return "DelegatingAuthenticator{" +
      "delegate=" + delegate +
      '}';
  }
}
