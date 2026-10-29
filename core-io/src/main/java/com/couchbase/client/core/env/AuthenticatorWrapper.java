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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.io.netty.kv.sasl.SaslCredentials;
import com.couchbase.client.core.io.netty.kv.sasl.SingleStepSaslAuthParameters;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import javax.net.ssl.KeyManagerFactory;

@NullMarked
@Stability.Internal
public abstract class AuthenticatorWrapper implements Authenticator {

  protected abstract Authenticator wrapped();

  @Override
  public @Nullable SaslCredentials keyValueSaslCredentials(final EndpointContext endpointContext) {
    return wrapped().keyValueSaslCredentials(endpointContext);
  }

  @Override
  public @Nullable String getAuthHeaderValue() {
    return wrapped().getAuthHeaderValue();
  }

  @Override
  public @Nullable KeyManagerFactory getKeyManagerFactory() {
    return wrapped().getKeyManagerFactory();
  }

  @Override
  public boolean requiresTls() {
    return wrapped().requiresTls();
  }

  @Override
  public @Nullable SingleStepSaslAuthParameters getSingleStepSaslAuthParameters() {
    return wrapped().getSingleStepSaslAuthParameters();
  }
}
