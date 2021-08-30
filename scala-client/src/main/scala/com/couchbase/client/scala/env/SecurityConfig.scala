/*
 * Copyright (c) 2019 Couchbase, Inc.
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
package com.couchbase.client.scala.env

import java.security.cert.X509Certificate
import com.couchbase.client.core

import java.nio.file.Path
import java.security.KeyStore
import java.util.Optional
import javax.net.ssl.TrustManagerFactory
import scala.jdk.CollectionConverters._

case class TrustStoreFile(path: Path, password: String, storeType: Option[String])

case class SecurityConfig(
    private[scala] val tlsEnabled: Option[Boolean] = None,
    private[scala] val nativeTlsEnabled: Option[Boolean] = None,
    private[scala] val trustCertificates: Option[Seq[X509Certificate]] = None,
    private[scala] val trustManagerFactory: Option[TrustManagerFactory] = None,
    private[scala] val enableHostnameVerification: Option[Boolean] = None,
    private[scala] val trustCertificate: Option[Path] = None,
    private[scala] val ciphers: Option[Seq[String]] = None,
    private[scala] val trustKeyStore: Option[KeyStore] = None,
    private[scala] val trustStoreFile: Option[TrustStoreFile] = None
) {
  private[scala] def toCore: core.env.SecurityConfig.Builder = {
    val builder = new core.env.SecurityConfig.Builder

    tlsEnabled.foreach(v => builder.enableTls(v))
    nativeTlsEnabled.foreach(v => builder.enableNativeTls(v))
    trustCertificates.foreach(t => builder.trustCertificates(t.asJava))
    trustManagerFactory.foreach(v => builder.trustManagerFactory(v))
    enableHostnameVerification.foreach(v => builder.enableHostnameVerification(v))
    trustCertificate.foreach(v => builder.trustCertificate(v))
    ciphers.foreach(v => builder.ciphers(v.asJava))
    trustKeyStore.foreach(v => builder.trustStore(v))
    trustStoreFile.foreach(
      v => builder.trustStore(v.path, v.password, Optional.ofNullable(v.storeType.orNull))
    )
    builder
  }

  def enableTls(value: Boolean): SecurityConfig = {
    copy(tlsEnabled = Some(value))
  }

  def enableNativeTls(value: Boolean): SecurityConfig = {
    copy(nativeTlsEnabled = Some(value))
  }

  def enableHostnameVerification(value: Boolean): SecurityConfig = {
    copy(enableHostnameVerification = Some(value))
  }

  def trustCertificates(values: Seq[X509Certificate]): SecurityConfig = {
    copy(trustCertificates = Some(values))
  }

  def trustManagerFactory(value: TrustManagerFactory): SecurityConfig = {
    copy(trustManagerFactory = Some(value))
  }

  def trustCertificate(value: Path): SecurityConfig = {
    copy(trustCertificate = Some(value))
  }

  def trustStore(value: KeyStore): SecurityConfig = {
    copy(trustKeyStore = Some(value))
  }

  def trustStore(
      trustStorePath: Path,
      trustStorePassword: String,
      trustStoreType: Option[String] = None
  ): SecurityConfig = {
    copy(trustStoreFile = Some(TrustStoreFile(trustStorePath, trustStorePassword, trustStoreType)))
  }

  def ciphers(value: Seq[String]): SecurityConfig = {
    copy(ciphers = Some(value))
  }
}
