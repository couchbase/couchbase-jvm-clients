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


package com.couchbase.client.core.io;

import org.junit.jupiter.api.Test;

import static com.couchbase.client.core.io.TrafficCaptureMasking.maskAuthorization;
import static com.couchbase.client.core.io.TrafficCaptureMasking.maskSecretFields;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class TrafficCaptureMaskingTest {

  private static final String PASSWORD = "s3cretPassw0rd";

  @Test
  void masksSecretFormFields() {
    String body = "name=myLink&clientKey=-----BEGIN%20KEY-----&password=" + PASSWORD;
    assertEquals("name=myLink&clientKey=<masked>&password=<masked>", maskSecretFields(body));
  }

  @Test
  void doesNotMaskFormFieldsThatMerelyEndWithSecretName() {
    String body = "notpassword=visible&password_hint=visible";
    assertSame(body, maskSecretFields(body));
  }

  @Test
  void masksSecretJsonFields() {
    String body = "{\"url\":\"https://example.com\",\"auth_type\":\"basic\",\"username\":\"bob\","
      + "\"password\" : \"" + PASSWORD + "\",\"bearer_key\":\"tok\\\"en\",\"other\":\"visible\"}";
    assertEquals(
      "{\"url\":\"https://example.com\",\"auth_type\":\"basic\",\"username\":\"bob\","
        + "\"password\" : \"<masked>\",\"bearer_key\":\"<masked>\",\"other\":\"visible\"}",
      maskSecretFields(body)
    );
  }

  @Test
  void masksAuthorizationKeepingTheScheme() {
    assertEquals("Basic <masked>", maskAuthorization("Basic dXNlcjpwYXNz"));
    assertEquals("<masked>", maskAuthorization("opaqueToken123"));
  }
}
