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
import org.jspecify.annotations.Nullable;

import static com.couchbase.client.core.util.CbStrings.nullToEmpty;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class SingleStepSaslAuthParameters {
  private final SaslMechanism mechanism;
  private final String payload;

  private SingleStepSaslAuthParameters(SaslMechanism mechanism, String payload) {
    if (mechanism.roundtrips() != 1) {
      throw new IllegalArgumentException(mechanism + " is not allowed here because it does not have exactly one round trip.");
    }

    this.mechanism = requireNonNull(mechanism);
    this.payload = requireNonNull(payload);
  }

  public SaslMechanism mechanism() {
    return mechanism;
  }

  public String payload() {
    return payload;
  }

  public static SingleStepSaslAuthParameters plain(
    @Nullable String authorizationId,
    String username,
    String password
  ) {
    String payload = nullToEmpty(authorizationId) + "\0" + username + "\0" + password;
    return new SingleStepSaslAuthParameters(SaslMechanism.PLAIN, payload);
  }

  public static SingleStepSaslAuthParameters oauthbearer(
    @Nullable String authorizationId,
    String token
  ) {
    String authorizationComponent = authorizationId == null ? "" : "a=" + authorizationId;
    String payload = ("n," + authorizationComponent + ",\1auth=Bearer " + token + "\1\1");
    return new SingleStepSaslAuthParameters(SaslMechanism.OAUTHBEARER, payload);
  }

  @Override
  public String toString() {
    return "SingleStepSaslAuthParameters{" +
      "mechanism=" + mechanism +
      ", payload=<redacted>" + // Payload is sensitive, so MUST prevent it from appearing in logs.
      '}';
  }
}
