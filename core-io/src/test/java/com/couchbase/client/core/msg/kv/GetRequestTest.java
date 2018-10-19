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

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.msg.ResponseStatus;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.kv.ProtocolVerifier.decodeHexDump;
import static com.couchbase.client.utils.Utils.readResource;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Verifies the encoding and decoding of the {@link GetRequest}.
 *
 * @since 2.0.0
 */
class GetRequestTest {

  private static final Duration TIMEOUT = Duration.ZERO;

  @Test
  void decodeSuccessfulResponse() {
    ByteBuf response = decodeHexDump(readResource(
      "get_response_success.txt",
      GetRequestTest.class
    ));

    GetRequest request = new GetRequest(null, TIMEOUT, null);
    GetResponse decoded = request.decode(response);

    byte[] expected = ("{\"callsign\":\"AIRCALIN\",\"country\":\"France\"," +
      "\"iata\":\"SB\",\"icao\":\"ACI\",\"id\":139," +
      "\"name\":\"Air Caledonie International\",\"type\":\"airline\"}"
    ).getBytes(CharsetUtil.UTF_8);

    assertEquals(ResponseStatus.SUCCESS, decoded.status());
    assertArrayEquals(expected, decoded.content());
  }

  @Test
  void decodeNotFoundResponse() {
    ByteBuf response = decodeHexDump(readResource(
      "get_response_not_found.txt",
      GetRequestTest.class
    ));

    GetRequest request = new GetRequest(null, TIMEOUT, null);
    GetResponse decoded = request.decode(response);

    assertEquals(ResponseStatus.NOT_FOUND, decoded.status());
    assertNull(decoded.content());
  }

}