/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.client.kotlin.env.dsl

import com.couchbase.client.core.env.SecurityConfig
import com.couchbase.client.core.env.SecurityConfig.Defaults.DEFAULT_HOSTNAME_VERIFICATION_ENABLED
import com.couchbase.client.core.env.SecurityConfig.Defaults.DEFAULT_NATIVE_TLS_ENABLED
import com.couchbase.client.core.env.SecurityConfig.Defaults.DEFAULT_TLS_ENABLED
import com.couchbase.client.kotlin.internal.toOptional
import java.nio.file.Path
import java.security.KeyStore
import java.security.cert.X509Certificate
import javax.net.ssl.TrustManagerFactory
import kotlin.properties.Delegates.observable

/**
 * DSL counterpart to [SecurityConfig.Builder].
 */
@ClusterEnvironmentDslMarker
public class SecurityConfigDslBuilder(private val wrapped: SecurityConfig.Builder) {
    /**
     * @see SecurityConfig.Builder.enableTls
     */
    public var enableTls: Boolean
            by observable(DEFAULT_TLS_ENABLED) { _, _, it -> wrapped.enableTls(it) }

    /**
     * @see SecurityConfig.Builder.enableHostnameVerification
     */
    public var enableHostnameVerification: Boolean
            by observable(DEFAULT_HOSTNAME_VERIFICATION_ENABLED) { _, _, it -> wrapped.enableHostnameVerification(it) }

    /**
     * @see SecurityConfig.Builder.enableNativeTls
     */
    public var enableNativeTls: Boolean
            by observable(DEFAULT_NATIVE_TLS_ENABLED) { _, _, it -> wrapped.enableNativeTls(it) }

    /**
     * Specifies where the trusted certificates come from.
     */
    public var trust: TrustSource
            by observable(TrustSource.trustNoOne()) { _, _, it -> it.configureBuilder(wrapped) }
}

/**
 * Provides access to trusted certificates for TLS.
 */
public interface TrustSource {
    public fun configureBuilder(builder: SecurityConfig.Builder)

    public companion object {
        /**
         * @see SecurityConfig.Builder.trustStore
         */
        public fun trustStore(store: KeyStore): TrustSource = from {
            trustStore(store)
        }

        /**
         * @see SecurityConfig.Builder.trustStore
         */
        public fun trustStore(path: Path, password: String, type: String = KeyStore.getDefaultType()): TrustSource =
            from {
                trustStore(path, password, type.toOptional())
            }

        /**
         * @see SecurityConfig.Builder.trustCertificates
         */
        public fun certificates(certificates: List<X509Certificate>): TrustSource = from {
            trustCertificates(certificates)
        }

        /**
         * @see SecurityConfig.Builder.trustCertificate
         */
        public fun certificate(certificatePath: Path): TrustSource = from {
            trustCertificate(certificatePath)
        }

        /**
         * @see SecurityConfig.Builder.trustManagerFactory
         */
        public fun factory(trustManagerFactory: TrustManagerFactory): TrustSource = from {
            trustManagerFactory(trustManagerFactory)
        }

        /**
         * Equivalent to not configuring the trust settings.
         */
        internal fun trustNoOne(): TrustSource = from {}

        private fun from(initializer: SecurityConfig.Builder.() -> Unit): TrustSource {
            return object : TrustSource {
                override fun configureBuilder(builder: SecurityConfig.Builder) {
                    builder.initializer()
                }
            }
        }

    }
}

