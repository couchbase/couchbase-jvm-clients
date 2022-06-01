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

import org.junit.jupiter.api.Test;

import javax.naming.CommunicationException;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link DnsSrv} helper class.
 */
class DnsSrvTest {

  @Test
  void loadDnsRecords() throws Exception {
    String service = "_couchbase._tcp.couchbase.com";
    BasicAttributes basicAttributes = new BasicAttributes(true);
    BasicAttribute basicAttribute = new BasicAttribute("SRV");
    basicAttribute.add("0 0 0 node2.couchbase.com.");
    basicAttribute.add("0 0 0 node1.couchbase.com.");
    basicAttribute.add("0 0 0 node3.couchbase.com.");
    basicAttribute.add("0 0 0 node4.couchbase.com.");
    basicAttributes.put(basicAttribute);
    DirContext mockedContext = mock(DirContext.class);
    when(mockedContext.getAttributes(service, new String[] { "SRV" }))
      .thenReturn(basicAttributes);

    List<String> records = DnsSrv.loadDnsRecords(service, mockedContext);
    assertEquals(4, records.size());
    assertEquals("node2.couchbase.com", records.get(0));
    assertEquals("node1.couchbase.com", records.get(1));
    assertEquals("node3.couchbase.com", records.get(2));
    assertEquals("node4.couchbase.com", records.get(3));
  }

  @Test
  void throwsNameNotFoundWhenMissingSrvRecord() throws Exception {
    NamingException e = assertThrows(NamingException.class, () -> DnsSrv.fromDnsSrv("localhost", true, false));
    if (e instanceof CommunicationException) {
      // this is fine, prevents failing this test when run without internet connection
      ignoreTest("Failed to contact DNS server.");
    }
    if (!(e instanceof NameNotFoundException)) {
      fail("Expected NameNotFoundException but got " + e.getClass());
    }
  }

  @Test
  void bootstrapFromDnsSrv() throws Exception {
    try {
      String demoService = "_xmpp-server._tcp.gmail.com";
      String publicNameServer = "8.8.8.8"; //google's public DNS
      List<String> strings = DnsSrv.fromDnsSrv(demoService, true, false, publicNameServer);
      assertTrue(strings.size() > 0);
    } catch (CommunicationException ex) {
      // this is fine, prevents failing this test when run without internet connection
      ignoreTest("Failed to contact DNS server.");
    }
  }

  private static void ignoreTest(String message) {
    assumeTrue(false, message);
  }

}
