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

package com.couchbase.client.core.util;

import com.couchbase.client.core.deps.org.xbill.DNS.lookup.ServerFailedException;
import org.junit.jupiter.api.Test;

import javax.naming.NameNotFoundException;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Verifies the functionality of the {@link DnsSrv} helper class.
 */
class DnsSrvTest {

  @Test
  void throwsNameNotFoundWhenMissingSrvRecord() throws Exception {
    Exception e = assertThrows(Exception.class, () -> DnsSrv.fromDnsSrv("localhost", true, false));
    if (isNetworkFailure(e)) {
      // this is fine, prevents failing this test when run without internet connection or in weird CI environment
      ignoreTest("Failed to contact DNS server: " + e);
    }
    if (!(e instanceof NameNotFoundException)) {
      fail("Expected NameNotFoundException but got " + e.getClass());
    }
  }

  @Test
  void bootstrapFromDnsSrv() throws Exception {
    try {
      // Any public well-known DNS SRV record suffices here
      String demoService = "_x-puppet._tcp.dnscheck.co.";
      String publicNameServer = "8.8.8.8"; //google's public DNS
      List<HostAndPort> addresses = new DnsSrvResolver(ForkJoinPool.commonPool(), publicNameServer)
        .resolve(demoService)
        .block();

      assertNotNull(addresses);
      assertFalse(addresses.isEmpty());

    } catch (RuntimeException e) {
      if (isNetworkFailure(e)) {
        // this is fine, prevents failing this test when run without internet connection or in weird CI environment
        ignoreTest("Failed to contact DNS server: " + e);
      } else {
        throw e;
      }
    }
  }

  private static boolean isNetworkFailure(Exception e) {
    return CbThrowables.hasCause(e, SocketException.class) || CbThrowables.hasCause(e, ServerFailedException.class);
  }

  private static void ignoreTest(String message) {
    assumeTrue(false, message);
  }

}
