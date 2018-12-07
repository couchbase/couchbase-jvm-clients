package com.couchbase.client.core.service.util;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.util.ConnectionString;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectionStringTest {

  @Test
  void shouldParseValidSchemes() {
    ConnectionString parsed = ConnectionString.create("couchbase://");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertTrue(parsed.allHosts().isEmpty());
    assertTrue(parsed.params().isEmpty());

    parsed = ConnectionString.create("couchbases://");
    assertEquals(ConnectionString.Scheme.COUCHBASES, parsed.scheme());
    assertTrue(parsed.allHosts().isEmpty());
    assertTrue(parsed.params().isEmpty());

    parsed = ConnectionString.create("http://");
    assertEquals(ConnectionString.Scheme.HTTP, parsed.scheme());
    assertTrue(parsed.allHosts().isEmpty());
    assertTrue(parsed.params().isEmpty());
  }

  @Test
  void shouldFailOnInvalidScheme() {
    assertThrows(CouchbaseException.class, () -> ConnectionString.create("invalid://"));
  }

  @Test
  void shouldParseHostList() {
    ConnectionString parsed = ConnectionString.create("couchbase://localhost");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertTrue(parsed.params().isEmpty());
    assertEquals(1, parsed.allHosts().size());
    assertFalse(parsed.allHosts().get(0).isUnresolved());
    assertEquals("localhost", parsed.allHosts().get(0).getHostName());
    assertEquals("127.0.0.1", parsed.allHosts().get(0).getAddress().getHostAddress());
    assertEquals(0, parsed.allHosts().get(0).getPort());
    assertEquals(null, parsed.username());

    parsed = ConnectionString.create("couchbase://localhost:1234");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertTrue(parsed.params().isEmpty());
    assertEquals(1, parsed.allHosts().size());
    assertEquals("localhost", parsed.allHosts().get(0).getHostName());
    assertEquals(1234, parsed.allHosts().get(0).getPort());
    assertEquals(null, parsed.username());

    parsed = ConnectionString.create("couchbase://foo:1234,bar:5678");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertTrue(parsed.params().isEmpty());
    assertEquals(2, parsed.allHosts().size());
    assertEquals("foo", parsed.allHosts().get(0).getHostName());
    assertEquals(1234, parsed.allHosts().get(0).getPort());
    assertEquals("bar", parsed.allHosts().get(1).getHostName());
    assertEquals(5678, parsed.allHosts().get(1).getPort());
    assertEquals(null, parsed.username());

    parsed = ConnectionString.create("couchbase://foo,bar:5678,baz");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertTrue(parsed.params().isEmpty());
    assertEquals(3, parsed.allHosts().size());
    assertEquals("foo", parsed.allHosts().get(0).getHostName());
    assertEquals(0, parsed.allHosts().get(0).getPort());
    assertEquals("bar", parsed.allHosts().get(1).getHostName());
    assertEquals(5678, parsed.allHosts().get(1).getPort());
    assertEquals("baz", parsed.allHosts().get(2).getHostName());
    assertEquals(0, parsed.allHosts().get(2).getPort());
    assertEquals(null, parsed.username());
  }

  @Test
  void shouldParseParams() {
    ConnectionString parsed = ConnectionString.create("couchbase://localhost?foo=bar");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertEquals(1, parsed.allHosts().size());
    assertEquals(1, parsed.params().size());
    assertEquals("bar", parsed.params().get("foo"));
    assertEquals(null, parsed.username());

    parsed = ConnectionString.create("couchbase://localhost?foo=bar&setting=true");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertEquals(1, parsed.allHosts().size());
    assertEquals(2, parsed.params().size());
    assertEquals("bar", parsed.params().get("foo"));
    assertEquals("true", parsed.params().get("setting"));
    assertEquals(null, parsed.username());
  }

  @Test
  public void shouldParseUsername() {
    ConnectionString parsed = ConnectionString.create("couchbase://user@localhost?foo=bar");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertEquals("user", parsed.username());
    assertEquals(new InetSocketAddress("localhost", 0), parsed.allHosts().get(0));
    assertEquals(1, parsed.params().size());
    assertEquals("bar", parsed.params().get("foo"));

    parsed = ConnectionString.create("couchbase://user123@host1,host2?foo=bar&setting=true");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertEquals("user123", parsed.username());
    assertEquals(new InetSocketAddress("host1", 0), parsed.allHosts().get(0));
    assertEquals(new InetSocketAddress("host2", 0), parsed.allHosts().get(1));
    assertEquals(2, parsed.params().size());
    assertEquals("bar", parsed.params().get("foo"));
    assertEquals("true", parsed.params().get("setting"));
  }

  @Test
  public void shouldAcceptSingleIPv6WithoutPort() {
    ConnectionString parsed = ConnectionString.create("couchbase://[::1]");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertEquals(1, parsed.allHosts().size());
    assertEquals(new InetSocketAddress("::1", 0), parsed.allHosts().get(0));
    assertTrue(parsed.params().isEmpty());

    parsed = ConnectionString.create("couchbase://[::1/128]");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertEquals(1, parsed.allHosts().size());
    assertEquals(new InetSocketAddress("::1/128", 0), parsed.allHosts().get(0));
    assertTrue(parsed.params().isEmpty());
  }

  @Test
  public void shouldAcceptMultipleIPv6WithoutPort() {
    ConnectionString parsed = ConnectionString.create("couchbase://[::1], [::1]");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertEquals(2, parsed.allHosts().size());
    assertEquals(new InetSocketAddress("::1", 0), parsed.allHosts().get(0));
    assertEquals(new InetSocketAddress("::1", 0), parsed.allHosts().get(1));
    assertTrue(parsed.params().isEmpty());

    parsed = ConnectionString.create("couchbase://[::1/128], [::1/128],[::1/128]");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertEquals(3, parsed.allHosts().size());
    assertEquals(new InetSocketAddress("::1/128", 0), parsed.allHosts().get(0));
    assertEquals(new InetSocketAddress("::1/128", 0), parsed.allHosts().get(1));
    assertEquals(new InetSocketAddress("::1/128", 0), parsed.allHosts().get(2));
    assertTrue(parsed.params().isEmpty());
  }

  @Test
  public void shouldAcceptSingleIPv6WithPort() {
    ConnectionString parsed = ConnectionString.create("couchbases://[::1]:8091, [::1]:11210");
    assertEquals(ConnectionString.Scheme.COUCHBASES, parsed.scheme());
    assertEquals(2, parsed.allHosts().size());
    assertEquals(new InetSocketAddress("::1", 8091), parsed.allHosts().get(0));
    assertEquals(new InetSocketAddress("::1", 11210), parsed.allHosts().get(1));
    assertTrue(parsed.params().isEmpty());

    parsed = ConnectionString.create("couchbase://[::1/128]:1234, [::1/128]:11210,[::1/128]:1");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertEquals(3, parsed.allHosts().size());
    assertEquals(new InetSocketAddress("::1/128", 1234), parsed.allHosts().get(0));
    assertEquals(new InetSocketAddress("::1/128", 11210), parsed.allHosts().get(1));
    assertEquals(new InetSocketAddress("::1/128", 1), parsed.allHosts().get(2));
    assertTrue(parsed.params().isEmpty());
  }

}