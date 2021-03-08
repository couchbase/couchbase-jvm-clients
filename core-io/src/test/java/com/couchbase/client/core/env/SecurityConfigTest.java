/*
 * Copyright (c) 2021 Couchbase, Inc.
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

import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.io.netty.SslHandlerFactory;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SecurityConfigTest {

  @Test
  void listsDefaultCiphersForNonNativeTls() {
    List<String> ciphers = SecurityConfig.defaultCiphers(false);
    assertFalse(ciphers.isEmpty());
    for (String cipher : ciphers) {
      assertTrue(cipher.startsWith("TLS_"));
    }
  }

  @Test
  void listsDefaultCiphersForNativeTls() {
    if (SslHandlerFactory.opensslAvailable()) {
      List<String> ciphers = SecurityConfig.defaultCiphers(true);
      assertFalse(ciphers.isEmpty());
      for (String cipher : ciphers) {
        assertTrue(cipher.startsWith("TLS_"));
      }
    } else {
      assertThrows(InvalidArgumentException.class, () -> SecurityConfig.defaultCiphers(true));
    }
  }

}