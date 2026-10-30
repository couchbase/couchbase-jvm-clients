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

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.couchbase.client.core.util.CbCollections.setOf;
import static java.util.Objects.requireNonNull;

/**
 * Rejects a server certificate chain that the trust manager it wraps accepts, if the chain includes a blocked
 * certificate: one that's known to be compromised, but that a JVM's default trust store may still trust (through
 * the root that issued it). Otherwise delegates everything to the wrapped trust manager.
 * <p>
 * The blocked certificates are the ones OkHttp 5.5.1 blocks (its {@code CertificateBlocklist}): eight compromised
 * eMudhra intermediate certificates (see
 * <a href="https://groups.google.com/a/mozilla.org/g/dev-security-policy/c/0R3XLJL6FuE/m/0XofdebrAAAJ">Mozilla's
 * announcement</a>). OkHttp checks them only for its own connections; wrapping the trust manager covers every TLS
 * connection that uses it, including KV connections that use NIO.
 * <p>
 * Like OkHttp, identifies a certificate by the SHA-256 hash of its public key (its encoded SubjectPublicKeyInfo),
 * and checks the certificates the server sent.
 * <p>
 * Thread-safe, if the wrapped trust manager is.
 */
@NullMarked
final class BlockedCertificateTrustManager extends X509ExtendedTrustManager {

  /**
   * SHA-256 hashes of the blocked certificates' public keys (hex).
   */
  static final Set<String> BLOCKED_PUBLIC_KEY_SHA256 = setOf(
    // Compromised eMudhra intermediates
    // https://groups.google.com/a/mozilla.org/g/dev-security-policy/c/0R3XLJL6FuE/m/0XofdebrAAAJ
    "43bffa19d6ef651451f6841e019931e96f6f2eaeff28ab4b6f1ce4576de2717b",
    "49dd353dc2b919a3770645117d0e60c16115e7d1bc259eefe734b00ea434fe5f",
    "5c0660a2b164910d8ff589c18e14552e9e0ec237dcefe2cb736d41a4811f81a0",
    "63ecefee431d49523bc1136c4e72d0206a477086caf606cd5cf025686c109cf8",
    "92c063d67e8f14db71d288e11266ffdaf69cbf115f7bab14f3bc2f68d25e0e30",
    "e0254c7fcdc265e2f0f8a428dabb75ab4ae039b4872d62f5ee57f0877d5a8a41",
    "ebed6475c2c2d80fcb199f3fe8dd6e6b3ce60b77eb15d4e42c3dec0c6f5b5105",
    "ec4ffa1ad0ac88081314b7aaffef6a91f97cd57291bb5ba686c5415f5eb4f6b6"
  );

  private final X509ExtendedTrustManager delegate;
  private final Set<String> blocked;

  private BlockedCertificateTrustManager(X509ExtendedTrustManager delegate, Set<String> blocked) {
    this.delegate = requireNonNull(delegate);
    this.blocked = requireNonNull(blocked);
  }

  /**
   * Returns the trust manager, wrapped, blocking {@link #BLOCKED_PUBLIC_KEY_SHA256}.
   */
  static X509TrustManager wrap(X509TrustManager trustManager) {
    return wrap(trustManager, BLOCKED_PUBLIC_KEY_SHA256);
  }

  /**
   * Returns the trust manager, wrapped. Like {@link CertificateLoggingTrustManager#wrap}, an
   * {@link X509ExtendedTrustManager} gets a wrapper that's one too, and a plain {@link X509TrustManager} gets a
   * plain wrapper (so the JDK still adds its own checks around it).
   *
   * @param blocked SHA-256 hashes of the blocked certificates' public keys (lowercase hex).
   */
  static X509TrustManager wrap(
    X509TrustManager trustManager,
    Set<String> blocked
  ) {
    return trustManager instanceof X509ExtendedTrustManager
      ? new BlockedCertificateTrustManager((X509ExtendedTrustManager) trustManager, blocked)
      : new Plain(trustManager, blocked);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, @Nullable Socket socket) throws CertificateException {
    delegate.checkServerTrusted(chain, authType, socket);
    rejectBlocked(chain, blocked);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, @Nullable SSLEngine engine) throws CertificateException {
    delegate.checkServerTrusted(chain, authType, engine);
    rejectBlocked(chain, blocked);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    delegate.checkServerTrusted(chain, authType);
    rejectBlocked(chain, blocked);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, @Nullable Socket socket) throws CertificateException {
    delegate.checkClientTrusted(chain, authType, socket);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, @Nullable SSLEngine engine) throws CertificateException {
    delegate.checkClientTrusted(chain, authType, engine);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    delegate.checkClientTrusted(chain, authType);
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return delegate.getAcceptedIssuers();
  }

  @Override
  public String toString() {
    return "BlockedCertificateTrustManager{" + delegate + "}";
  }

  /**
   * The wrapper for a plain {@link X509TrustManager}.
   */
  static final class Plain implements X509TrustManager {
    private final X509TrustManager delegate;
    private final Set<String> blocked;

    private Plain(X509TrustManager delegate, Set<String> blocked) {
      this.delegate = requireNonNull(delegate);
      this.blocked = requireNonNull(blocked);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      delegate.checkServerTrusted(chain, authType);
      rejectBlocked(chain, blocked);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      delegate.checkClientTrusted(chain, authType);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return delegate.getAcceptedIssuers();
    }

    @Override
    public String toString() {
      return "BlockedCertificateTrustManager.Plain{" + delegate + "}";
    }
  }

  /**
   * Throws if any of the certificates is blocked.
   */
  private static void rejectBlocked(
    X509Certificate[] chain,
    Set<String> blocked
  ) throws CertificateException {
    for (X509Certificate certificate : chain) {
      String publicKeySha256 = publicKeySha256(certificate);
      if (blocked.contains(publicKeySha256)) {
        throw new CertificateException(
          "The server's certificate chain includes a blocked (compromised) certificate: " + certificate.getSubjectX500Principal() + " with public key sha256 " + publicKeySha256
        );
      }
    }
  }

  /**
   * Returns the SHA-256 hash of the certificate's public key (its encoded SubjectPublicKeyInfo), in lowercase hex.
   */
  static String publicKeySha256(X509Certificate certificate) {
    try {
      byte[] hash = MessageDigest.getInstance("SHA-256").digest(certificate.getPublicKey().getEncoded());
      StringBuilder hex = new StringBuilder(hash.length * 2);
      for (byte b : hash) {
        hex.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e); // every JVM supports SHA-256
    }
  }
}
