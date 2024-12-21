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

package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;
import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

import java.security.SecureRandom;

/**
 * Integrates the Couchbase infrastructure with {@link BlockHound}.
 */
@Stability.Internal
public final class CouchbaseBlockHoundIntegration implements BlockHoundIntegration {

  @Override
  public void applyTo(final BlockHound.Builder builder) {
    // We need to generate UUIDs in a couple places, which reads bytes blocking from the PRNG
    builder.allowBlockingCallsInside(SecureRandom.class.getName(), "nextBytes");
  }

}
