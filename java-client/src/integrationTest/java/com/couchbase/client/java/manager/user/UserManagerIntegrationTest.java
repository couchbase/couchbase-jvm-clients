/*
 * Copyright 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.manager.user;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import com.couchbase.client.test.Services;
import com.couchbase.client.test.TestNodeConfig;
import com.couchbase.mock.deps.org.apache.http.auth.AuthScope;
import com.couchbase.mock.deps.org.apache.http.auth.UsernamePasswordCredentials;
import com.couchbase.mock.deps.org.apache.http.client.CredentialsProvider;
import com.couchbase.mock.deps.org.apache.http.client.methods.CloseableHttpResponse;
import com.couchbase.mock.deps.org.apache.http.client.methods.HttpGet;
import com.couchbase.mock.deps.org.apache.http.impl.client.BasicCredentialsProvider;
import com.couchbase.mock.deps.org.apache.http.impl.client.CloseableHttpClient;
import com.couchbase.mock.deps.org.apache.http.impl.client.HttpClientBuilder;
import com.couchbase.mock.deps.org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.java.manager.user.AuthDomain.LOCAL;
import static com.couchbase.client.java.manager.user.UpsertUserOptions.upsertUserOptions;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
class UserManagerIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static ClusterEnvironment environment;

  private static UserManager users;
  private static GroupManager groups;

  private static final String USERNAME = "integration-test-user";

  private static final Role ADMIN = new Role("admin");
  private static final Role READ_ONLY_ADMIN = new Role("ro_admin");
  private static final Role BUCKET_FULL_ACCESS_WILDCARD = new Role("bucket_full_access", "*");

  @BeforeAll
  static void setup() {
    environment = environment().build();
    cluster = Cluster.connect(environment);
    users = cluster.users();
    groups = cluster.users().groups();
  }

  @AfterAll
  static void tearDown() {
    cluster.shutdown();
    environment.shutdown();
  }

  @AfterEach
  @BeforeEach
  void dropTestUser() {
    dropUserQuietly(USERNAME);
    assertUserAbsent(USERNAME);
  }

  @Test
  void availableRoles() {
    List<RoleAndDescription> roles = users.availableRoles();

    // Full results vary by server version, but should at least contain the admin role.
    assertTrue(roles.stream()
        .map(RoleAndDescription::role)
        .anyMatch(role -> role.equals(ADMIN)));
  }

  @Test
  void getAll() {
    users.create(new User(USERNAME)
        .displayName("Integration Test User")
        .roles(ADMIN), "password");

    assertTrue(users.getAll().stream()
        .anyMatch(meta -> meta.user().username().equals(USERNAME)));
  }

  @Test
  void createWithBadRole() {
    assertThrows(CouchbaseException.class, () -> {
      users.create(new User(USERNAME)
          .displayName("Integration Test User")
          .roles(new Role("bogus")), "password");
    });
  }

  @Test
  void create() {
    final String origPassword = "password";
    final String newPassword = "newpassword";

    users.create(new User(USERNAME)
        .displayName("Integration Test User")
        .roles(ADMIN), origPassword);

    // must be a specific kind of admin for this to succeed (not exactly sure which)
    assertCanAuthenticate(USERNAME, origPassword);

    UserAndMetadata userMeta = users.get(LOCAL, USERNAME);
    assertEquals(LOCAL, userMeta.domain());
    assertEquals("Integration Test User", userMeta.user().displayName());
    Set<Role> expectedRoles = singleton(ADMIN);
    assertEquals(expectedRoles, userMeta.innateRoles());
    assertEquals(expectedRoles, userMeta.user().roles());
    assertEquals(expectedRoles, userMeta.effectiveRoles());

    checkRoleOrigins(userMeta, "admin<-[user]");

    users.upsert(new User(USERNAME)
        .displayName("Renamed")
        .roles(ADMIN));

    assertCanAuthenticate(USERNAME, origPassword);

    users.upsert(
        new User(USERNAME)
            .displayName("Renamed")
            .roles(READ_ONLY_ADMIN, BUCKET_FULL_ACCESS_WILDCARD),
        upsertUserOptions().password(newPassword));

    assertCanAuthenticate(USERNAME, newPassword);

    userMeta = users.get(LOCAL, USERNAME);
    assertEquals("Renamed", userMeta.user().displayName());
    assertEquals(setOf(READ_ONLY_ADMIN, BUCKET_FULL_ACCESS_WILDCARD), userMeta.innateRoles());

    checkRoleOrigins(userMeta, "ro_admin<-[user]", "bucket_full_access[*]<-[user]");
  }

  static void checkRoleOrigins(UserAndMetadata userMeta, String... expected) {
    Set<String> expectedRolesAndOrigins = setOf(expected);
    Set<String> actualRolesAndOrigns = userMeta.effectiveRolesAndOrigins().stream().map(Object::toString).collect(Collectors.toSet());
    assertEquals(expectedRolesAndOrigins, actualRolesAndOrigns);
  }

  private void assertCanAuthenticate(String username, String password) {

    CredentialsProvider provider = new BasicCredentialsProvider();
    provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

    try (CloseableHttpClient client = HttpClientBuilder.create()
        .setDefaultCredentialsProvider(provider)
        .build()) {

      TestNodeConfig node = config().nodes().get(0);
      String hostAndPort = node.hostname() + ":" + node.ports().get(Services.MANAGER);

      try (CloseableHttpResponse response = client.execute(new HttpGet("http://" + hostAndPort + "/pools"))) {
        String body = EntityUtils.toString(response.getEntity());
        assertEquals(200, response.getStatusLine().getStatusCode(), "Server response: " + body);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void dropUserQuietly(String name) {
    try {
      users.drop(name);
    } catch (UserNotFoundException e) {
      // that's fine!
    }
  }

  private static void dropGroupQuietly(String name) {
    try {
      groups.drop(name);
    } catch (GroupNotFoundException e) {
      // that's fine!
    }
  }

  private static void assertUserAbsent(String username) {
    // there are two ways to do this!

    assertFalse(users.getAll().stream()
        .map(UserAndMetadata::user)
        .anyMatch(user -> user.username().equals(username)));

    assertThrows(UserNotFoundException.class, () -> users.get(LOCAL, username));
  }

  @Test
  void dropAbsentUser() {
    String name = "doesnotexist";
    UserNotFoundException e = assertThrows(UserNotFoundException.class, () -> users.drop(name));
    assertEquals(name, e.username());
    assertEquals(LOCAL, e.domain());
  }
}
