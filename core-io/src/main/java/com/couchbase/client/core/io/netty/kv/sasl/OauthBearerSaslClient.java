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
import com.couchbase.client.core.env.SaslMechanism;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslException;

import static com.couchbase.client.core.io.netty.kv.sasl.CallbackHelper.getPassword;
import static java.nio.charset.StandardCharsets.UTF_8;

@Stability.Internal
@NullMarked
public class OauthBearerSaslClient extends InitialResponseSaslClient {
  public OauthBearerSaslClient(
    @Nullable String authorizationId,
    CallbackHandler callbackHandler
  ) {
    super(authorizationId, callbackHandler);
  }

  @Override
  public String getMechanismName() {
    return SaslMechanism.OAUTHBEARER.mech();
  }

  @Override
  protected byte[] getInitialResponse(CallbackHandler callbackHandler) throws SaslException {
    String authorizationComponent = authorizationId == null ? "" : "a=" + authorizationId;
    String token = getPassword(callbackHandler);
    return ("n," + authorizationComponent + ",\1auth=Bearer " + token + "\1\1").getBytes(UTF_8);
  }
}
