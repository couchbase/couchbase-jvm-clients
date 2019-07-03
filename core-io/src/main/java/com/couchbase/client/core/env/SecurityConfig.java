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

import com.couchbase.client.core.annotation.Stability;

import javax.net.ssl.TrustManagerFactory;
import java.security.cert.X509Certificate;
import java.util.LinkedHashMap;
import java.util.Map;

public class SecurityConfig {

  public static final boolean DEFAULT_NATIVE_TLS_ENABLED = true;

  private final boolean nativeTlsEnabled;
  private final boolean tlsEnabled;
  private final boolean certAuthEnabled;
  private final X509Certificate[] trustCertificates;
  private final TrustManagerFactory trustManagerFactory;

  public static Builder builder() {
    return new Builder();
  }

  public static SecurityConfig create() {
    return new SecurityConfig(builder());
  }

  public static Builder tlsEnabled(boolean tlsEnabled) {
    return builder().tlsEnabled(tlsEnabled);
  }

  public static Builder certAuthEnabled(boolean certAuthEnabled) {
    return builder().certAuthEnabled(certAuthEnabled);
  }

  public static Builder trustCertificates(final X509Certificate... certificates) {
    return builder().trustCertificates(certificates);
  }

  public static Builder trustManagerFactory(final TrustManagerFactory trustManagerFactory) {
    return builder().trustManagerFactory(trustManagerFactory);
  }

  private SecurityConfig(final Builder builder) {
    tlsEnabled = builder.tlsEnabled;
    nativeTlsEnabled = builder.nativeTlsEnabled;
    certAuthEnabled = builder.certAuthEnabled;
    trustCertificates = builder.trustCertificates;
    trustManagerFactory = builder.trustManagerFactory;

    if (tlsEnabled) {
      if (trustCertificates != null && trustManagerFactory != null) {
        throw new IllegalArgumentException("Either trust certificates or a trust manager factory" +
          "can be provided, but not both!");
      }
      if ((trustCertificates == null || trustCertificates.length == 0) && trustManagerFactory == null) {
        throw new IllegalArgumentException("Either a trust certificate or a trust manager factory" +
          "must be provided when TLS is enabled!");
      }
    }
  }

  public boolean certAuthEnabled() {
    return certAuthEnabled;
  }

  public boolean tlsEnabled() {
    return tlsEnabled;
  }

  public X509Certificate[] trustCertificates() {
    return trustCertificates;
  }

  public TrustManagerFactory trustManagerFactory() {
    return trustManagerFactory;
  }

  public boolean nativeTlsEnabled() {
    return nativeTlsEnabled;
  }

  /**
   * Returns this config as a map so it can be exported into i.e. JSON for display.
   */
  @Stability.Volatile
  Map<String, Object> exportAsMap() {
    Map<String, Object> export = new LinkedHashMap<>();
    export.put("tlsEnabled", tlsEnabled);
    export.put("nativeTlsEnabled", nativeTlsEnabled);
    export.put("certAuthEnabled", certAuthEnabled);
    export.put("hasTrustCertificates", trustCertificates != null && trustCertificates.length > 0);
    export.put("trustManagerFactory", trustManagerFactory != null ? trustManagerFactory.getClass().getSimpleName() : null);
    return export;
  }

  public static class Builder {

    private boolean tlsEnabled = false;
    private boolean nativeTlsEnabled = DEFAULT_NATIVE_TLS_ENABLED;
    private boolean certAuthEnabled = false;
    private X509Certificate[] trustCertificates = null;
    private TrustManagerFactory trustManagerFactory = null;

    public SecurityConfig build() {
      return new SecurityConfig(this);
    }

    public Builder tlsEnabled(boolean tlsEnabled) {
      this.tlsEnabled = tlsEnabled;
      return this;
    }

    public Builder nativeTlsEnabled(boolean nativeTlsEnabled) {
      this.nativeTlsEnabled = nativeTlsEnabled;
      return this;
    }

    public Builder certAuthEnabled(boolean certAuthEnabled) {
      throw new UnsupportedOperationException("not yet supported");
      // this.certAuthEnabled = certAuthEnabled;
      // return this;
    }

    public Builder trustCertificates(final X509Certificate... certificates) {
      this.trustCertificates = certificates;
      return this;
    }

    public Builder trustManagerFactory(final TrustManagerFactory trustManagerFactory) {
      this.trustManagerFactory = trustManagerFactory;
      return this;
    }
  }
}
