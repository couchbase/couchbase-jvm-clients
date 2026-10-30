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

package com.couchbase.client.core.deps.io.netty.handler.ssl.util;

import javax.net.ssl.TrustManagerFactory;

/**
 * Holds a {@link TrustManagerFactory} that trusts every certificate, without checking it.
 * <p>
 * <strong>For testing only.</strong> A connection that trusts every certificate isn't secure.
 *
 * @deprecated Scheduled for removal. Instead, please disable certificate verification by setting
 * {@link com.couchbase.client.core.env.SecurityConfig.Builder#enableCertificateVerification(boolean) security.enableCertificateVerification}
 * to false.
 */
@Deprecated
public final class InsecureTrustManagerFactory {
  /**
   * The same instance as {@link com.couchbase.client.core.env.InsecureTrustManagerFactory#INSTANCE}.
   */
  public static final TrustManagerFactory INSTANCE = com.couchbase.client.core.env.InsecureTrustManagerFactory.INSTANCE;

  private InsecureTrustManagerFactory() {
  }
}
