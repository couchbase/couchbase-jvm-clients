/*
 * Copyright (c) 2018 Couchbase, Inc.
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

import java.time.Duration;
import java.util.Set;

/**
 * The {@link IoEnvironment} holds all IO-related configuration and state.
 *
 * @since 2.0.0
 */
public interface IoEnvironment {

  /**
   * The full timeout for a channel to be established, includes the
   * socket connect as well all the back and forth depending on the
   * service used.
   *
   * @return the full connect timeout for a channel.
   */
  Duration connectTimeout();

  /**
   * Configures the way {@link CompressionConfig} is set up in the client.
   *
   * @return the compression settings.
   */
  CompressionConfig compressionConfig();

  /**
   * Configures the way transport layer security is set up in the client.
   *
   * @return the security settings.
   */
  SecurityConfig securityConfig();

  /**
   * Customizes the SASL mechanisms that are allowed to be negotiated, even
   * if the server would support more/different ones.
   *
   * @return the set of mechanisms allowed.
   */
  Set<SaslMechanism> allowedSaslMechanisms();

}
