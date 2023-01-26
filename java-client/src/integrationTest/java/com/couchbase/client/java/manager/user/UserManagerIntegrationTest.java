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
import com.couchbase.client.core.error.UserNotFoundException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.core.util.ConsistencyUtil;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Services;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.Util;
import com.couchbase.client.test.IgnoreWhen;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.java.manager.user.AuthDomain.LOCAL;
import static com.couchbase.client.java.util.GroupUserManagementUtil.dropUserQuietly;
import static com.couchbase.client.test.Capabilities.COLLECTIONS;
import static com.couchbase.client.test.Capabilities.ENTERPRISE_EDITION;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IgnoreWhen(clusterTypes = {ClusterType.MOCKED, ClusterType.CAVES, ClusterType.CAPELLA},
  isProtostellarWillWorkLater = true
)
class UserManagerIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;

  private static UserManager users;

  private static final String USERNAME = "integration-test-user";

  private static final Role ADMIN = new Role("admin");
  private static final Role READ_ONLY_ADMIN = new Role("ro_admin");
  private static final Role BUCKET_FULL_ACCESS_WILDCARD = new Role("bucket_full_access", "*");

  @BeforeAll
  static void setup() {
    cluster = createCluster();
    Bucket bucket = cluster.bucket(config().bucketname());
    users = cluster.users();
    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }

  @AfterEach
  @BeforeEach
  void dropTestUser() {
    dropUserQuietly(cluster.core(), users, USERNAME);
    assertUserAbsent(USERNAME);
  }

  private void upsert(User user) {
    users.upsertUser(user);
    ConsistencyUtil.waitUntilUserPresent(cluster.core(), LOCAL.alias(), user.username());
  }

  @Test
  @IgnoreWhen(missesCapabilities = ENTERPRISE_EDITION)
  void changeUserPassword() {
    final String username = "changePasswordTestUser";
    final String origPassword = "password";
    final String newPassword = "newpassword";

    upsert(new User(username)
            .password(origPassword)
            .roles(ADMIN));

    ClusterOptions options = ClusterOptions.clusterOptions(username, origPassword);

    Cluster disposableCluster = Cluster.connect(connectionString(), options);
    disposableCluster.waitUntilReady(java.time.Duration.ofSeconds(30));
    UserManager disposableUserManager = disposableCluster.users();
    Bucket disposableBucket = disposableCluster.bucket(config().bucketname());

    disposableUserManager.changePassword(newPassword);

    assertCanAuthenticate(username, newPassword, true);
    assertCanAuthenticate(username, origPassword, false);

    disposableCluster.disconnect();
    users.dropUser(username);
    ConsistencyUtil.waitUntilUserDropped(cluster.core(), AuthDomain.LOCAL.alias(), username);
  }

  @Test
  @IgnoreWhen(hasCapabilities = ENTERPRISE_EDITION)
  void changeUserPasswordShouldFailOnCE() {
    final String username = "changePasswordTestUser";
    final String origPassword = "password";
    final String newPassword = "newpassword";

    upsert(new User(username)
            .password(origPassword)
            .roles(ADMIN));

    ClusterOptions options = ClusterOptions.clusterOptions(username, origPassword);

    Cluster disposableCluster = Cluster.connect(connectionString(), options);
    disposableCluster.waitUntilReady(java.time.Duration.ofSeconds(30));
    UserManager disposableUserManager = disposableCluster.users();
    Bucket disposableBucket = disposableCluster.bucket(config().bucketname());

    assertThrows(FeatureNotAvailableException.class, () -> disposableUserManager.changePassword(newPassword));

    disposableCluster.disconnect();
    users.dropUser(username);
    ConsistencyUtil.waitUntilUserDropped(cluster.core(), AuthDomain.LOCAL.alias(), username);
  }

  @Test
  void getRoles() {
    List<RoleAndDescription> roles = users.getRoles();

    // Full results vary by server version, but should at least contain the admin role.
    assertTrue(roles.stream()
        .map(RoleAndDescription::role)
        .anyMatch(role -> role.equals(ADMIN)));
  }

  @Test
  void getAll() {
    upsert(new User(USERNAME)
        .password("password")
        .displayName("Integration Test User")
        .roles(ADMIN));

    assertTrue(users.getAllUsers().stream()
        .anyMatch(meta -> meta.user().username().equals(USERNAME)));
  }

  @Test
  void createWithBadRole() {
    assertThrows(CouchbaseException.class, () ->
        users.upsertUser(new User(USERNAME)
            .password("password")
            .displayName("Integration Test User")
            .roles(new Role("bogus"))));
  }

  @Test
  @IgnoreWhen(missesCapabilities = {COLLECTIONS, ENTERPRISE_EDITION})// No RBAC support for CE
  void canAssignCollectionsAwareRoles() {
    String bucket = config().bucketname();
    assertCanCreateWithRole(new Role("data_reader", bucket));
    assertCanCreateWithRole(new Role("data_reader", bucket, "_default", null));
    assertCanCreateWithRole(new Role("data_reader", bucket, "_default", "_default"));
  }

  private void assertCanCreateWithRole(Role role) {
    upsert(new User(USERNAME)
        .password("password")
        .displayName("Integration Test User")
        .roles(role));

    assertEquals(setOf(role), users.getUser(LOCAL, USERNAME).effectiveRoles());
  }

  @Test
  void create() {
    final String origPassword = "password";
    final String newPassword = "newpassword";

    upsert(new User(USERNAME)
        .password(origPassword)
        .displayName("Integration Test User")
        .roles(ADMIN));

    // must be a specific kind of admin for this to succeed (not exactly sure which)
    assertCanAuthenticate(USERNAME, origPassword, true);

    UserAndMetadata userMeta = users.getUser(LOCAL, USERNAME);
    assertEquals(LOCAL, userMeta.domain());
    assertEquals("Integration Test User", userMeta.user().displayName());
    Set<Role> expectedRoles = singleton(ADMIN);
    assertEquals(expectedRoles, userMeta.innateRoles());
    assertEquals(expectedRoles, userMeta.user().roles());
    assertEquals(expectedRoles, userMeta.effectiveRoles());

    checkRoleOrigins(userMeta, "admin<-[user]");

    users.upsertUser(new User(USERNAME)
        .displayName("Renamed")
        .roles(ADMIN));

    Util.waitUntilCondition(() -> {
      UserAndMetadata user = users.getUser(LOCAL, USERNAME);
      return user.user().displayName().equals("Renamed");
    });

    assertCanAuthenticate(USERNAME, origPassword, true);

    users.upsertUser(
        new User(USERNAME)
            .displayName("Renamed")
            .roles(READ_ONLY_ADMIN, BUCKET_FULL_ACCESS_WILDCARD)
            .password(newPassword));
    Util.waitUntilCondition(() -> {
      UserAndMetadata user = users.getUser(LOCAL, USERNAME);
      return user.user().roles().size() == 2;
    });

    assertCanAuthenticate(USERNAME, newPassword, true);

    userMeta = users.getUser(LOCAL, USERNAME);
    assertEquals("Renamed", userMeta.user().displayName());
    assertEquals(setOf(READ_ONLY_ADMIN, BUCKET_FULL_ACCESS_WILDCARD), userMeta.innateRoles());

    checkRoleOrigins(userMeta, "ro_admin<-[user]", "bucket_full_access[*]<-[user]");
  }

  static void checkRoleOrigins(UserAndMetadata userMeta, String... expected) {
    Set<String> expectedRolesAndOrigins = setOf(expected);
    Set<String> actualRolesAndOrigns = userMeta.effectiveRolesAndOrigins().stream().map(Object::toString).collect(Collectors.toSet());
    assertEquals(expectedRolesAndOrigins, actualRolesAndOrigns);
  }

  private void assertCanAuthenticate(String username, String password, boolean expectSuccess) {

    CredentialsProvider provider = new BasicCredentialsProvider();
    provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

    try (CloseableHttpClient client = HttpClientBuilder.create()
        .setDefaultCredentialsProvider(provider)
        .build()) {

      TestNodeConfig node = config().nodes().get(0);
      String hostAndPort = node.hostname() + ":" + node.ports().get(Services.MANAGER);

      try (CloseableHttpResponse response = client.execute(new HttpGet("http://" + hostAndPort + "/pools"))) {
        String body = EntityUtils.toString(response.getEntity());
        if (expectSuccess) {
          assertEquals(200, response.getStatusLine().getStatusCode(), "Server response: " + body);
        }
        else {
          assertNotEquals(200, response.getStatusLine().getStatusCode(), "Server response: " + body);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void assertUserAbsent(String username) {
    // there are two ways to do this!

    assertFalse(users.getAllUsers().stream()
        .map(UserAndMetadata::user)
        .anyMatch(user -> user.username().equals(username)));

    assertThrows(UserNotFoundException.class, () -> users.getUser(LOCAL, username));
  }

  @Test
  void dropAbsentUser() {
    String name = "doesnotexist";
    UserNotFoundException e = assertThrows(UserNotFoundException.class, () -> users.dropUser(name));
    assertEquals(name, e.username());
    assertEquals(LOCAL.alias(), e.domain());
  }
}
