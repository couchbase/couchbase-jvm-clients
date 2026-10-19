/*
 * Copyright (c) 2026 Couchbase, Inc.
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

package com.couchbase.client.core.io;

import com.couchbase.client.core.annotation.Stability;
import org.jspecify.annotations.NullMarked;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Masks credentials in captured HTTP traffic (see {@code io.servicesToCapture}). Shared by the Netty endpoints'
 * {@code TrafficCaptureHandler} and the OkHttp services' {@code TrafficCaptureInterceptor}, so both mask the same
 * things the same way.
 */
@NullMarked
@Stability.Internal
public final class TrafficCaptureMasking {
  private TrafficCaptureMasking() {
  }

  /**
   * What a masked value is replaced with.
   */
  public static final String MASKED = "<masked>";

  /**
   * Names of request body fields whose values are secrets. Covers user management (password),
   * analytics links (S3, Azure, GCS, remote Couchbase), and eventing URL bindings.
   */
  private static final String SECRET_FIELD_NAMES = String.join("|",
    "password",
    "clientKey",
    "clientSecret",
    "clientCertificatePassword",
    "secretAccessKey",
    "sessionToken",
    "accountKey",
    "sharedAccessSignature",
    "connectionString",
    "jsonCredentials",
    "bearer_key"
  );

  /**
   * Matches a secret field in a form-encoded body. Form values are percent-encoded,
   * so they can't contain a raw '&amp;' or line break.
   */
  private static final Pattern SECRET_FORM_FIELD = Pattern.compile(
    "(?im)(^|&)(" + SECRET_FIELD_NAMES + ")=[^&\\r\\n]*"
  );

  /**
   * Matches a secret field with a string value in a JSON body.
   */
  private static final Pattern SECRET_JSON_FIELD = Pattern.compile(
    "(?i)(\"(?:" + SECRET_FIELD_NAMES + ")\"\\s*:\\s*)\"(?:[^\"\\\\]|\\\\.)*\""
  );

  /**
   * Matches the auth scheme at the start of an Authorization header's value (kept when the credential is masked).
   */
  private static final Pattern AUTHORIZATION_SCHEME = Pattern.compile(
    "^[!#$%&'*+.^_`|~0-9a-zA-Z-]+[ \\t]+"
  );

  /**
   * Returns the text with the values of known secret fields masked, in form-encoded or JSON request bodies,
   * or the same instance if there are none.
   */
  public static String maskSecretFields(String text) {
    String masked = replaceAll(SECRET_FORM_FIELD, text, "$1$2=" + MASKED);
    return replaceAll(SECRET_JSON_FIELD, masked, "$1\"" + MASKED + "\"");
  }

  /**
   * Returns an Authorization header's value with the credential masked, keeping the auth scheme (if any).
   */
  public static String maskAuthorization(String value) {
    Matcher scheme = AUTHORIZATION_SCHEME.matcher(value);
    return scheme.find() ? scheme.group() + MASKED : MASKED;
  }

  /**
   * Like {@link Matcher#replaceAll(String)}, but returns the input instance if nothing matched.
   */
  public static String replaceAll(Pattern pattern, String input, String replacement) {
    Matcher matcher = pattern.matcher(input);
    return matcher.find() ? matcher.replaceAll(replacement) : input;
  }
}
