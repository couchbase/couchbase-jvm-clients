/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.core.diagnostics;

import com.couchbase.client.core.annotation.Stability;
import reactor.util.annotation.Nullable;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class InternalEndpointDiagnostics {
  public final EndpointDiagnostics internal;
  public final AuthenticationStatus authenticationStatus;
  @Nullable public final Throwable tlsHandshakeFailure;

  public InternalEndpointDiagnostics(
    EndpointDiagnostics internal,
    AuthenticationStatus authenticationStatus,
    @Nullable Throwable tlsHandshakeFailure
  ) {
    this.internal = requireNonNull(internal);
    this.authenticationStatus = requireNonNull(authenticationStatus);
    this.tlsHandshakeFailure = tlsHandshakeFailure;
  }
}
