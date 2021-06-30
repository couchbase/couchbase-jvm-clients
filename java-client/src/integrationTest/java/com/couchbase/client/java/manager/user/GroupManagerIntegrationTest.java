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

import com.couchbase.client.core.error.GroupNotFoundException;
import com.couchbase.client.core.error.UserNotFoundException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import com.couchbase.client.test.Util;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.java.manager.user.AuthDomain.LOCAL;
import static com.couchbase.client.java.manager.user.UserManagerIntegrationTest.checkRoleOrigins;
import static com.couchbase.client.test.Capabilities.COLLECTIONS;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@IgnoreWhen(clusterTypes =ClusterType.MOCKED, missesCapabilities = {Capabilities.USER_GROUPS, Capabilities.ENTERPRISE_EDITION})
class GroupManagerIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;

  private static UserManager users;

  private static final Role READ_ONLY_ADMIN = new Role("ro_admin");
  private static final Role BUCKET_FULL_ACCESS_WILDCARD = new Role("bucket_full_access", "*");
  private static final Role SECURITY_ADMIN = new Role("security_admin");
  private static final Role SECURITY_ADMIN_LOCAL = new Role("security_admin_local");


  private static final String USERNAME = "integration-test-user";
  private static final String GROUP_A = "group-a";
  private static final String GROUP_B = "group-b";

  @BeforeAll
  static void setup() {
    cluster = Cluster.connect(seedNodes(), clusterOptions());
    users = cluster.users();
    cluster.waitUntilReady(Duration.ofSeconds(5));
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }

  @AfterEach
  @BeforeEach
  void dropTestUser() {
    dropUserQuietly(USERNAME);
    dropGroupQuietly(GROUP_A);
    dropGroupQuietly(GROUP_B);
    waitUntilUserDropped(USERNAME);
    waitUntilGroupDropped(GROUP_A);
    waitUntilGroupDropped(GROUP_B);
  }

  private void waitUntilUserPresent(String name) {
    Util.waitUntilCondition(() -> {
      try {
        users.getUser(LOCAL, name);
        return true;
      }
      catch (UserNotFoundException err) {
        return false;
      }
    });
  }

  private void waitUntilUserDropped(String name) {
    Util.waitUntilCondition(() -> {
      try {
        users.getUser(LOCAL, name);
        return false;
      }
      catch (UserNotFoundException err) {
        return true;
      }
    });
  }

  private void waitUntilGroupPresent(String name) {
    Util.waitUntilCondition(() -> {
      try {
        users.getGroup(name);
        return true;
      }
      catch (GroupNotFoundException err) {
        return false;
      }
    });
  }

  private void waitUntilGroupDropped(String name) {
    Util.waitUntilCondition(() -> {
      try {
        users.getGroup(name);
        return false;
      }
      catch (GroupNotFoundException err) {
        return true;
      }
    });
  }

  @Test
  void getAll() {
    users.upsertGroup(new Group(GROUP_A));
    users.upsertGroup(new Group(GROUP_B));

    waitUntilGroupPresent(GROUP_A);
    waitUntilGroupPresent(GROUP_B);

    Set<String> actualNames = users.getAllGroups().stream()
        .map(Group::name)
        .collect(Collectors.toSet());

    assertTrue(actualNames.containsAll(setOf(GROUP_A, GROUP_B)));

    users.dropGroup(GROUP_B);

    waitUntilGroupDropped(GROUP_B);

    assertFalse(users.getAllGroups().stream()
        .map(Group::name)
        .anyMatch(name -> name.equals(GROUP_B)));
  }

  @Test
  @IgnoreWhen(hasCapabilities = COLLECTIONS)
  void createPreCheshireCat() {
    create(SECURITY_ADMIN);
  }

  @Test
  @IgnoreWhen(missesCapabilities = COLLECTIONS)
  void createPostCheshireCat() {
    create(SECURITY_ADMIN_LOCAL);
  }

  void create(Role securityAdmin) {
    final String fakeLdapRef = "ou=Users";
    upsert(new Group(GROUP_A).description("a").roles(READ_ONLY_ADMIN).ldapGroupReference(fakeLdapRef));
    upsert(new Group(GROUP_B).description("b").roles(READ_ONLY_ADMIN, BUCKET_FULL_ACCESS_WILDCARD));

    assertEquals("a", users.getGroup(GROUP_A).description());
    assertEquals("b", users.getGroup(GROUP_B).description());

    assertEquals(Optional.of(fakeLdapRef), users.getGroup(GROUP_A).ldapGroupReference());
    assertEquals(Optional.empty(), users.getGroup(GROUP_B).ldapGroupReference());

    assertEquals(setOf(READ_ONLY_ADMIN), users.getGroup(GROUP_A).roles());
    assertEquals(setOf(READ_ONLY_ADMIN, BUCKET_FULL_ACCESS_WILDCARD), users.getGroup(GROUP_B).roles());

    upsert(new User(USERNAME)
        .password("password")
        .roles(securityAdmin, BUCKET_FULL_ACCESS_WILDCARD)
        .groups(GROUP_A, GROUP_B));

    UserAndMetadata userMeta = users.getUser(AuthDomain.LOCAL, USERNAME);

    assertEquals(setOf(securityAdmin, BUCKET_FULL_ACCESS_WILDCARD), userMeta.user().roles());
    assertEquals(setOf(securityAdmin, BUCKET_FULL_ACCESS_WILDCARD, READ_ONLY_ADMIN), userMeta.effectiveRoles());

    // xxx possibly flaky, depends on order of origins reported by server?
    checkRoleOrigins(userMeta,
      securityAdmin.name() +"<-[user]",
        "ro_admin<-[group:group-a, group:group-b]",
        "bucket_full_access[*]<-[group:group-b, user]");

    users.upsertGroup(users.getGroup(GROUP_A).roles(securityAdmin));
    users.upsertGroup(users.getGroup(GROUP_B).roles(securityAdmin));

    Util.waitUntilCondition(() -> users.getGroup(GROUP_A).roles().size() == 1
              && users.getGroup(GROUP_B).roles().size() == 1);

    userMeta = users.getUser(AuthDomain.LOCAL, USERNAME);
    assertEquals(setOf(securityAdmin, BUCKET_FULL_ACCESS_WILDCARD), userMeta.effectiveRoles());
  }

  @Test
  void dropAbsentGroup() {
    String name = "doesnotexist";
    GroupNotFoundException e = assertThrows(GroupNotFoundException.class, () -> users.dropGroup(name));
    assertEquals(name, e.groupName());
  }

  @Test
  @IgnoreWhen(missesCapabilities = COLLECTIONS)
  void userInheritsCollectionAwareRoles() {
    String bucket = config().bucketname();
    assertUserInheritsRole(new Role("data_reader", bucket));
    assertUserInheritsRole(new Role("data_reader", bucket, "_default", null));
    assertUserInheritsRole(new Role("data_reader", bucket, "_default", "_default"));
  }

  private void assertUserInheritsRole(Role role) {
    upsert(new Group(GROUP_A).roles(role));
    assertEquals(setOf(role), users.getGroup(GROUP_A).roles());

    upsert(new User(USERNAME).password("password").groups(GROUP_A));
    assertEquals(setOf(role), users.getUser(LOCAL, USERNAME).effectiveRoles());
  }

  @Test
  void removeUserFromAllGroups() {
    // exercise the special-case code for upserting an empty group list.

    upsert(new Group(GROUP_A).roles(READ_ONLY_ADMIN));
    upsert(new User(USERNAME).password("password").groups(GROUP_A));

    UserAndMetadata userMeta = users.getUser(AuthDomain.LOCAL, USERNAME);
    assertEquals(setOf(READ_ONLY_ADMIN), userMeta.effectiveRoles());

    users.upsertUser(userMeta.user().groups(emptySet()));

    Util.waitUntilCondition(() -> users.getUser(LOCAL, USERNAME).externalGroups().isEmpty());

    userMeta = users.getUser(AuthDomain.LOCAL, USERNAME);
    assertEquals(emptySet(), userMeta.effectiveRoles());
  }

  private void upsert(Group group) {
    users.upsertGroup(group);
    waitUntilGroupPresent(group.name());
  }

  private void upsert(User user) {
    users.upsertUser(user);
    waitUntilUserPresent(user.username());
  }

  private static void dropUserQuietly(String name) {
    try {
      users.dropUser(name);
    } catch (UserNotFoundException e) {
      // that's fine!
    }
  }

  private static void dropGroupQuietly(String name) {
    try {
      users.dropGroup(name);
    } catch (GroupNotFoundException e) {
      // that's fine!
    }
  }
}
