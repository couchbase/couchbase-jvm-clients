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
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.java.manager.user.UserManagerIntegrationTest.checkRoleOrigins;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@IgnoreWhen(clusterTypes = ClusterType.MOCKED, missesCapabilities = Capabilities.USER_GROUPS)
class GroupManagerIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static ClusterEnvironment environment;

  private static UserManager users;
  private static GroupManager groups;

  private static final Role READ_ONLY_ADMIN = new Role("ro_admin");
  private static final Role BUCKET_FULL_ACCESS_WILDCARD = new Role("bucket_full_access", "*");
  private static final Role SECURITY_ADMIN = new Role("security_admin");

  private static final String USERNAME = "integration-test-user";
  private static final String GROUP_A = "group-a";
  private static final String GROUP_B = "group-b";

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
    dropGroupQuietly(GROUP_A);
    dropGroupQuietly(GROUP_B);
  }

  @Test
  void getAll() {
    groups.upsert(new Group(GROUP_A));
    groups.upsert(new Group(GROUP_B));

    Set<String> actualNames = groups.getAll().stream()
        .map(Group::name)
        .collect(Collectors.toSet());

    assertTrue(actualNames.containsAll(setOf(GROUP_A, GROUP_B)));

    groups.drop(GROUP_B);

    assertFalse(groups.getAll().stream()
        .map(Group::name)
        .anyMatch(name -> name.equals(GROUP_B)));
  }

  @Test
  void create() {
    final String fakeLdapRef = "ou=Users";
    groups.upsert(new Group(GROUP_A).description("a").roles(READ_ONLY_ADMIN).ldapGroupReference(fakeLdapRef));
    groups.upsert(new Group(GROUP_B).description("b").roles(READ_ONLY_ADMIN, BUCKET_FULL_ACCESS_WILDCARD));

    assertEquals("a", groups.get(GROUP_A).description());
    assertEquals("b", groups.get(GROUP_B).description());

    assertEquals(Optional.of(fakeLdapRef), groups.get(GROUP_A).ldapGroupReference());
    assertEquals(Optional.empty(), groups.get(GROUP_B).ldapGroupReference());

    assertEquals(setOf(READ_ONLY_ADMIN), groups.get(GROUP_A).roles());
    assertEquals(setOf(READ_ONLY_ADMIN, BUCKET_FULL_ACCESS_WILDCARD), groups.get(GROUP_B).roles());

    users.create(new User(USERNAME)
            .roles(SECURITY_ADMIN, BUCKET_FULL_ACCESS_WILDCARD)
            .groups(GROUP_A, GROUP_B),
        "password");

    UserAndMetadata userMeta = users.get(AuthDomain.LOCAL, USERNAME);

    assertEquals(setOf(SECURITY_ADMIN, BUCKET_FULL_ACCESS_WILDCARD), userMeta.innateRoles());
    assertEquals(setOf(SECURITY_ADMIN, BUCKET_FULL_ACCESS_WILDCARD, READ_ONLY_ADMIN), userMeta.effectiveRoles());

    // xxx possibly flaky, depends on order of origins reported by server?
    checkRoleOrigins(userMeta,
        "security_admin<-[user]",
        "ro_admin<-[group:group-a, group:group-b]",
        "bucket_full_access[*]<-[group:group-b, user]");

    groups.upsert(groups.get(GROUP_A).roles(SECURITY_ADMIN));
    groups.upsert(groups.get(GROUP_B).roles(SECURITY_ADMIN));

    userMeta = users.get(AuthDomain.LOCAL, USERNAME);
    assertEquals(setOf(SECURITY_ADMIN, BUCKET_FULL_ACCESS_WILDCARD), userMeta.effectiveRoles());
  }

  @Test
  void dropAbsentGroup() {
    String name = "doesnotexist";
    GroupNotFoundException e = assertThrows(GroupNotFoundException.class, () -> groups.drop(name));
    assertEquals(name, e.groupName());
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
}
