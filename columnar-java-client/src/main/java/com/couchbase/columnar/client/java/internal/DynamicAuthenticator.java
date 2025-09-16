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

package com.couchbase.columnar.client.java.internal;

import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.AuthenticatorWrapper;
import org.jetbrains.annotations.ApiStatus;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Delegates all methods to the authenticator returned by the
 * given supplier.
 */
@ApiStatus.Internal
public class DynamicAuthenticator extends AuthenticatorWrapper {
  private final Supplier<Authenticator> supplier;

  public DynamicAuthenticator(Supplier<Authenticator> supplier) {
    this.supplier = requireNonNull(supplier);
  }

  @Override
  protected Authenticator wrapped() {
    return supplier.get();
  }
}
