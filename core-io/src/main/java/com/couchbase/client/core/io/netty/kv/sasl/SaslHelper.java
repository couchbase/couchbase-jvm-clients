/*
 * Copyright 2023 Couchbase, Inc.
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

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;

public class SaslHelper {
  private SaslHelper() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Returns true iff {@link Sasl#createSaslClient} supports the PLAIN mechanism.
   * <p>
   * PLAIN is always supported, unless the JVM runs in a restricted mode
   * that prevents plaintext authentication.
   * <p>
   * We could easily provide our own SASL PLAIN implementation, but for now
   * we're honoring the platform restriction.
   */
  public static boolean platformHasSaslPlain() {
    return PLATFORM_HAS_SASL_PLAIN;
  }

  private static final boolean PLATFORM_HAS_SASL_PLAIN = initPlatformHasSaslPlain();

  private static boolean initPlatformHasSaslPlain() {
    try {
      return null != Sasl.createSaslClient(
        new String[]{"PLAIN"},
        null,
        "couchbase",
        "example.com",
        null,
        callbackHandler("dummyUsername", "dummyPassword")
      );
    } catch (Throwable t) {
      return false;
    }
  }

  private static CallbackHandler callbackHandler(String username, String password) {
    return callbacks -> {
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          ((NameCallback) callback).setName(username);
        } else if (callback instanceof PasswordCallback) {
          ((PasswordCallback) callback).setPassword(password.toCharArray());
        } else {
          throw new UnsupportedCallbackException(callback, "Unexpected/Unsupported Callback");
        }
      }
    };
  }
}
