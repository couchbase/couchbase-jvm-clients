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

package com.couchbase.client.core.retry;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.diagnostics.AuthenticationStatus;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.service.ServiceType;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Stability.Internal
public class AuthErrorDecider {
  /**
   * Determine based on the auth status of all GCCCP endpoints whether there's an authentication problem, such as bad credentials or a bad certificate.
   * <p>
   * GCCCP is used exclusively because it's the most reliable determiner.  Every cluster must have KV, every supported cluster must have GCCCP, and a bucket-associated KV connection can fail (NO_ACCESS)
   * for other reasons more to do with authorization than authentication - such as the user not having permissions to access the bucket.  Or reasons to do with neither, such as the bucket not existing
   * or being hibernated.
   * <p>
   * AuthenticationStatus.FAILED on a GCCCP endpoint should mean exactly that.
   */
  public static boolean isAuthError(List<EndpointDiagnostics> endpointDiagnostics) {
    // We decide AUTHENTICATION_ERROR if _any_ node's GCCCP stream is auth-erroring.  Because a) it's a fairly safe assumption that if one stream is auth-erroring the rest will be, and b) it allows the user
    // to override our decision in the RetryStrategy by redoing this logic and requiring a different heuristic (such as all or a majority of nodes to be auth-erroring).
    return endpointDiagnostics.stream().anyMatch(ed ->

      // Look for the GCCCP streams
      ed.type() == ServiceType.KV
        && !ed.namespace().isPresent()
        && ed.authenticationStatus() == AuthenticationStatus.FAILED);
  }

  public static boolean isAuthError(Stream<EndpointDiagnostics> endpointDiagnostics) {
    return isAuthError(endpointDiagnostics.collect(Collectors.toList()));
  }
}
