/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.columnar.cluster;


import com.couchbase.columnar.client.java.Cluster;
import com.couchbase.columnar.client.java.Credential;

import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;


public class ColumnarClusterConnection {
  private final Cluster cluster;
  private final fit.columnar.ClusterNewInstanceRequest request;
  // Commands to run when this ColumnarClusterConnection is being closed.  Allows closing other related resources that have
  // the same lifetime.
  private final List<Runnable> onClusterConnectionClose;

  public ColumnarClusterConnection(fit.columnar.ClusterNewInstanceRequest request,
                                   ArrayList<Runnable> onClusterConnectionClose) {
    this.request = request;
    this.onClusterConnectionClose = onClusterConnectionClose;

    if (!request.hasOptions()) {
      this.cluster = Cluster.newInstance(
        request.getConnectionString(),
        Credential.of(
          request.getCredential().getUsernameAndPassword().getUsername(),
          request.getCredential().getUsernameAndPassword().getPassword()
        )
      );
    } else {
      var options = request.getOptions();

      if (options.hasDeserializer()) {
        // Ignore. FIT driver can't specify alternate deserializers in a meaningful way.
      }

      this.cluster = Cluster.newInstance(
        request.getConnectionString(),
        Credential.of(
          request.getCredential().getUsernameAndPassword().getUsername(),
          request.getCredential().getUsernameAndPassword().getPassword()
        ),
        env -> env
          .timeout(timeout -> {
            if (options.hasTimeout()) {
              var timeoutOptions = options.getTimeout();
              if (timeoutOptions.hasConnectTimeout()) {
                timeout.connectTimeout(Duration.ofSeconds(timeoutOptions.getConnectTimeout().getSeconds()));
              }
              if (timeoutOptions.hasDispatchTimeout()) {
                // todo - check with David if intentional
                throw new UnsupportedOperationException("dispatchTimeout not exposed in SDK");
              }
              if (timeoutOptions.hasQueryTimeout()) {
                timeout.queryTimeout(Duration.ofSeconds(timeoutOptions.getQueryTimeout().getSeconds()));
              }
            }
          })
          .security(sec -> {
            if (options.hasSecurity()) {
              var secOptions = options.getSecurity();
              if (secOptions.hasTrustOnlyCapella()) {
                sec.trustOnlyCapella();
              }
              if (secOptions.hasTrustOnlyPemString()) {
                sec.trustOnlyPemString(secOptions.getTrustOnlyPemString());
              }
              if (secOptions.hasTrustOnlyPlatform()) {
                sec.trustOnlyJvm();
              }
              if (secOptions.hasVerifyServerCertificate()) {
                //noinspection deprecation
                sec.verifyServerCertificate(secOptions.getVerifyServerCertificate());
              }
              if (secOptions.getCipherSuitesCount() > 0) {
                sec.cipherSuites(secOptions.getCipherSuitesList());
              }
            }
          }));
    }
  }

  public Cluster cluster() {
    return cluster;
  }

  public void close() {
    cluster.close();
    onClusterConnectionClose.forEach(Runnable::run);
  }

  public fit.columnar.ClusterNewInstanceRequest request() {
    return request;
  }
}
