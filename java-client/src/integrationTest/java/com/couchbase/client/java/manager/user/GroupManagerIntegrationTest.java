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

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.java.manager.user.AuthDomain.LOCAL;
import static com.couchbase.client.java.manager.user.UserManagerIntegrationTest.checkRoleOrigins;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@IgnoreWhen(clusterTypes = ClusterType.MOCKED, missesCapabilities = Capabilities.USER_GROUPS)
class GroupManagerIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;

  private static UserManager users;

  private static final Role READ_ONLY_ADMIN = new Role("ro_admin");
  private static final Role BUCKET_FULL_ACCESS_WILDCARD = new Role("bucket_full_access", "*");
  private static final Role SECURITY_ADMIN = new Role("security_admin");

  private static final String USERNAME = "integration-test-user";
  private static final String GROUP_A = "group-a";
  private static final String GROUP_B = "group-b";

  @BeforeAll
  static void setup() {
    cluster = Cluster.connect(connectionString(), clusterOptions());
    users = cluster.users();
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
  void create() {
    final String fakeLdapRef = "ou=Users";
    users.upsertGroup(new Group(GROUP_A).description("a").roles(READ_ONLY_ADMIN).ldapGroupReference(fakeLdapRef));
    users.upsertGroup(new Group(GROUP_B).description("b").roles(READ_ONLY_ADMIN, BUCKET_FULL_ACCESS_WILDCARD));

    waitUntilGroupPresent(GROUP_A);
    waitUntilGroupPresent(GROUP_B);

    assertEquals("a", users.getGroup(GROUP_A).description());
    assertEquals("b", users.getGroup(GROUP_B).description());

    assertEquals(Optional.of(fakeLdapRef), users.getGroup(GROUP_A).ldapGroupReference());
    assertEquals(Optional.empty(), users.getGroup(GROUP_B).ldapGroupReference());

    assertEquals(setOf(READ_ONLY_ADMIN), users.getGroup(GROUP_A).roles());
    assertEquals(setOf(READ_ONLY_ADMIN, BUCKET_FULL_ACCESS_WILDCARD), users.getGroup(GROUP_B).roles());

    users.upsertUser(new User(USERNAME)
        .password("password")
        .roles(SECURITY_ADMIN, BUCKET_FULL_ACCESS_WILDCARD)
        .groups(GROUP_A, GROUP_B));

    waitUntilUserPresent(USERNAME);

    UserAndMetadata userMeta = users.getUser(AuthDomain.LOCAL, USERNAME);

    assertEquals(setOf(SECURITY_ADMIN, BUCKET_FULL_ACCESS_WILDCARD), userMeta.user().roles());
    assertEquals(setOf(SECURITY_ADMIN, BUCKET_FULL_ACCESS_WILDCARD, READ_ONLY_ADMIN), userMeta.effectiveRoles());

    // xxx possibly flaky, depends on order of origins reported by server?
    checkRoleOrigins(userMeta,
        "security_admin<-[user]",
        "ro_admin<-[group:group-a, group:group-b]",
        "bucket_full_access[*]<-[group:group-b, user]");

    users.upsertGroup(users.getGroup(GROUP_A).roles(SECURITY_ADMIN));
    users.upsertGroup(users.getGroup(GROUP_B).roles(SECURITY_ADMIN));

    Util.waitUntilCondition(() -> users.getGroup(GROUP_A).roles().size() == 1
              && users.getGroup(GROUP_B).roles().size() == 1);

    userMeta = users.getUser(AuthDomain.LOCAL, USERNAME);
    assertEquals(setOf(SECURITY_ADMIN, BUCKET_FULL_ACCESS_WILDCARD), userMeta.effectiveRoles());
  }

  @Test
  void dropAbsentGroup() {
    String name = "doesnotexist";
    GroupNotFoundException e = assertThrows(GroupNotFoundException.class, () -> users.dropGroup(name));
    assertEquals(name, e.groupName());
  }

  @Test
  void removeUserFromAllGroups() {
    // exercise the special-case code for upserting an empty group list.

    users.upsertGroup(new Group(GROUP_A).roles(READ_ONLY_ADMIN));
    waitUntilGroupPresent(GROUP_A);
    users.upsertUser(new User(USERNAME).password("password").groups(GROUP_A));
    waitUntilUserPresent(USERNAME);

    UserAndMetadata userMeta = users.getUser(AuthDomain.LOCAL, USERNAME);
    assertEquals(setOf(READ_ONLY_ADMIN), userMeta.effectiveRoles());

    users.upsertUser(userMeta.user().groups(emptySet()));

    Util.waitUntilCondition(() -> users.getUser(LOCAL, USERNAME).externalGroups().isEmpty());

    userMeta = users.getUser(AuthDomain.LOCAL, USERNAME);
    assertEquals(emptySet(), userMeta.effectiveRoles());
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
