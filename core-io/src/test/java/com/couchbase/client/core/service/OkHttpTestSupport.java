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

package com.couchbase.client.core.service;

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.PasswordAuthenticator;

import java.time.Duration;

/**
 * Helpers for tests of the OkHttp-based services.
 */
final class OkHttpTestSupport {
  private OkHttpTestSupport() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Returns an OkHttp client like the one the Core creates, with the environment's security config,
   * a password authenticator, and default connection settings.
   */
  static CouchbaseOkHttpClient newTestOkHttpClient(CoreEnvironment env) {
    return new CouchbaseOkHttpClient(
      Duration.ofSeconds(5),
      env.ioConfig(),
      env.ioEnvironment().nativeIoEnabled(),
      env.securityConfig(),
      PasswordAuthenticator.create("username", "password"),
      "test-user-agent"
    );
  }
}
