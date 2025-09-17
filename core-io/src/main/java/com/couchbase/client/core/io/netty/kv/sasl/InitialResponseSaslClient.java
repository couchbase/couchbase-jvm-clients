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

package com.couchbase.client.core.io.netty.kv.sasl;

import com.couchbase.client.core.annotation.Stability;
import org.jspecify.annotations.Nullable;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import static com.couchbase.client.core.io.netty.kv.sasl.CallbackHelper.getPassword;
import static com.couchbase.client.core.io.netty.kv.sasl.CallbackHelper.getUsername;
import static java.util.Objects.requireNonNull;

/**
 * Base class for mechanisms that have a single step: the initial response.
 */
@Stability.Internal
abstract class InitialResponseSaslClient implements SaslClient {
  private final CallbackHandler callbackHandler;
  private final String authorizationId;

  private boolean complete;

  public InitialResponseSaslClient(
    @Nullable String authorizationId,
    CallbackHandler callbackHandler
  ) {
    this.authorizationId = authorizationId == null ? "" : authorizationId;
    this.callbackHandler = requireNonNull(callbackHandler);
  }

  @Override
  public boolean hasInitialResponse() {
    return true;
  }

  @Override
  public byte[] evaluateChallenge(byte[] challenge) throws SaslException {
    if (isComplete()) {
      throw new IllegalStateException("Can't call evaluateChallenge after authentication is complete.");
    }

    complete = true;
    String username = getUsername(callbackHandler);
    String password = getPassword(callbackHandler);
    return getInitialResponse(authorizationId, username, password);
  }

  abstract byte[] getInitialResponse(
    String authorizationId,
    String username,
    String password
  );

  @Override
  public boolean isComplete() {
    return complete;
  }

  @Override
  public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
    requireComplete("unwrap");
    throw new IllegalStateException(getMechanismName() + " has neither integrity nor privacy");
  }

  @Override
  public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
    requireComplete("wrap");
    throw new IllegalStateException(getMechanismName() + " has neither integrity nor privacy");
  }

  @Override
  public Object getNegotiatedProperty(String propName) {
    requireComplete("getNegotiatedProperty");
    return null;
  }

  private void requireComplete(String methodName) {
    if (!isComplete()) {
      throw new IllegalStateException("Can't call " + methodName + " before authentication is complete.");
    }
  }

  @Override
  public void dispose() throws SaslException {
  }
}
