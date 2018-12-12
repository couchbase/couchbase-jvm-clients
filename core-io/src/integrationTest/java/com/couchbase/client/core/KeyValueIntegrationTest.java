package com.couchbase.client.core;

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.GetResponse;
import com.couchbase.client.core.msg.kv.InsertRequest;
import com.couchbase.client.core.msg.kv.InsertResponse;
import com.couchbase.client.core.util.CoreIntegrationTest;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

class KeyValueIntegrationTest extends CoreIntegrationTest {

  private Core core;
  private CoreEnvironment env;

  @BeforeEach
  void beforeEach() {
    env = environment().build();
    core = Core.create(env);
    core.openBucket(config().bucketname()).block();
  }

  @AfterEach
  void afterEach() {
    core.shutdown().block();
    env.shutdown(Duration.ofSeconds(1));
  }

  /**
   * Validate that an inserted document can be read subsequently.
   */
  @Test
  void insertAndGet() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "hello, world".getBytes(CharsetUtil.UTF_8);

    InsertRequest insertRequest = new InsertRequest(id, null, content, 0, 0,
      Duration.ofSeconds(1), core.context(), config().bucketname(), env.retryStrategy());
    core.send(insertRequest);

    InsertResponse insertResponse = insertRequest.response().get();
    assertTrue(insertResponse.status().success());

    GetRequest getRequest = new GetRequest(id, null, Duration.ofSeconds(1), core.context(),
      config().bucketname(), env.retryStrategy());
    core.send(getRequest);

    GetResponse getResponse = getRequest.response().get();
    assertTrue(getResponse.status().success());
    assertArrayEquals(content, getResponse.content());
    assertTrue(getResponse.cas() != 0);
  }

}
