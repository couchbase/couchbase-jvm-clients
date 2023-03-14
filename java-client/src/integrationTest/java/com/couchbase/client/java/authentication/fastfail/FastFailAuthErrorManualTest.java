/*
 * Copyright (c) 2023 Couchbase, Inc.
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

package com.couchbase.client.java.authentication.fastfail;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.user.Role;
import com.couchbase.client.java.manager.user.User;
import com.couchbase.client.java.util.JavaIntegrationTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;


/**
 * This is a test we cannot currently automate:
 * 1. Create a connection with user:password
 * 2. Run some ops.
 * 3. After a few seconds, it changes user's password.  Currently, this leaves the connection open so ops should continue successfully.
 * 4. Force the connection down.  This is the part that has to be done manually.  Can do this with docker with:
 *    $ docker exec -it couchbase bash
 *    (inside docker container)$ pkill memcached
 *    and/or
 *    (inside docker container)$ pkill cbq-engine
 * 5. Verify that ops are now failing with AuthenticationFailedException.
 */
@Disabled
class FastFailAuthErrorManualTest extends JavaIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(FastFailAuthErrorManualTest.class);

  @Test
  void test() throws InterruptedException {
    String initialPassword = "password";

    String username = "user"; // UUID.randomUUID().toString()
    User user = new User(username);
    user.password(initialPassword);
    user.roles(new Role("data_writer", "*", "*", "*"));

    // Reset any previous test
    try (Cluster cluster = Cluster.connect("couchbase://" + config().nodes().get(0).hostname(),
      ClusterOptions.clusterOptions(config().adminUsername(), config().adminPassword())
        .environment(env -> env.retryStrategy(FastFailOnAuthErrorRetryStrategy.INSTANCE)))) {
       cluster.users().upsertUser(user);
    }

    Cluster cluster = Cluster.connect("couchbase://" + config().nodes().get(0).hostname(),
      ClusterOptions.clusterOptions(user.username(), initialPassword)
        .environment(env -> env.retryStrategy(FastFailOnAuthErrorRetryStrategy.INSTANCE)));

    String newPassword = "newpassword";
    String id = UUID.randomUUID().toString();

    Collection collection = cluster.bucket(config().bucketname()).defaultCollection();

    long start = System.nanoTime();
    boolean changedPassword = false;

    while (true) {
      Thread.sleep(1000);

      if (!changedPassword && TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) >= 3) {
        LOGGER.info("Changing password - kill service(s) now");
        cluster.users().changePassword(newPassword);
        changedPassword = true;
      }

      try {
        collection.upsert(id, JsonObject.create());
        LOGGER.info("KV success");
      }
      catch (CouchbaseException err) {
        LOGGER.warn("KV:" + err);
      }

      try {
        cluster.query("SELECT 'Hello World' AS Greeting");
        LOGGER.info("Query success");
      }
      catch (CouchbaseException err) {
        LOGGER.warn("Query:" + err);
      }
    }
  }
}
