/*
 * Copyright 2022 Couchbase, Inc.
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
package com.couchbase.client.scala.manager

import com.couchbase.client.core.error.{
  CouchbaseException,
  GroupNotFoundException,
  UserNotFoundException
}
import com.couchbase.client.core.util.ConsistencyUtil
import com.couchbase.client.scala.manager.user._
import com.couchbase.client.scala.util.{CouchbasePickler, ScalaIntegrationTest}
import com.couchbase.client.scala.{Cluster, Collection, TestUtils}
import com.couchbase.client.test._
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

import scala.util.{Failure, Success}

@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(
  clusterTypes = Array(ClusterType.MOCKED),
  missesCapabilities = Array(Capabilities.USER_GROUPS, Capabilities.ENTERPRISE_EDITION)
)
@ManagementApiTest
class GroupManagerSpec extends ScalaIntegrationTest {
  private var cluster: Cluster   = _
  private var users: UserManager = _
  private var coll: Collection   = _

  private val Username                 = "integration-test-user"
  private val GroupA                   = "group-a"
  private val GroupB                   = "group-b"
  private val SecurityAdmin            = Role("security_admin")
  private val SecurityAdminLocal       = Role("security_admin_local")
  private val ReadOnlyAdmin            = Role("ro_admin")
  private val BucketFullAccessWildcard = Role("bucket_full_access", Some("*"))

  @BeforeAll
  def setup(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection
    users = cluster.users
    TestUtils.waitForNsServerToBeReady(cluster)
  }

  @AfterAll
  def tearDown(): Unit = {
    cluster.disconnect()
  }

  private def dropUserQuietly(name: String): Unit = {
    users
      .dropUser(name) match {
      case Success(value)                      =>
      case Failure(err: UserNotFoundException) =>
      case Failure(err)                        => throw err
    }
    waitUntilUserDropped(name)
  }

  private def dropGroupQuietly(name: String): Unit = {
    users
      .dropGroup(name) match {
      case Success(value)                       =>
      case Failure(err: GroupNotFoundException) =>
      case Failure(err: CouchbaseException)     =>
        // Janky workaround for a problem seen in CI where ns_server appears not to be
        // ready
        if (!err.getMessage.contains("Method Not Allowed")) throw err
      case Failure(err) => throw err
    }
    waitUntilGroupDropped(name)
  }

  private def waitUntilUserPresent(name: String): Unit = {
    ConsistencyUtil.waitUntilUserPresent(cluster.async.core, AuthDomain.Local.alias, name)
    Util.waitUntilCondition(() => {
      users.getUser(name) match {
        case Success(_) => true
        case _          => false
      }
    })
  }

  private def waitUntilUserDropped(name: String): Unit = {
    ConsistencyUtil.waitUntilUserDropped(cluster.async.core, AuthDomain.Local.alias, name)
    Util.waitUntilCondition(() => {
      users.getUser(name) match {
        case Failure(err: UserNotFoundException) => true
        case _                                   => false
      }
    })
  }

  private def waitUntilGroupPresent(name: String): Unit = {
    ConsistencyUtil.waitUntilGroupPresent(cluster.async.core, name)
    Util.waitUntilCondition(() => {
      users.getGroup(name) match {
        case Success(_) => true
        case _          => false
      }
    })
  }

  private def waitUntilGroupDropped(name: String): Unit = {
    ConsistencyUtil.waitUntilGroupDropped(cluster.async.core, name)
    Util.waitUntilCondition(() => {
      users.getGroup(name) match {
        case Failure(err: GroupNotFoundException) => true
        case _                                    => false
      }
    })
  }

  private def assertGroupAbsent(groupName: String): Unit = {
    val allUsers = users.getAllGroups().get
    assert(!allUsers.exists(_.name == groupName))
  }

  @AfterEach
  @BeforeEach
  def dropTestUser(): Unit = {
    dropUserQuietly(Username)
    dropGroupQuietly(GroupA)
    dropGroupQuietly(GroupB)
    assertGroupAbsent(GroupA)
    assertGroupAbsent(GroupB)
  }

  @Test
  def getAll(): Unit = {
    users.upsertGroup(new Group(GroupA)).get
    users.upsertGroup(new Group(GroupB)).get
    waitUntilGroupPresent(GroupA)
    waitUntilGroupPresent(GroupB)
    val actualNames = users.getAllGroups().get.map(_.name)
    assert(actualNames.contains(GroupA))
    assert(actualNames.contains(GroupB))
  }

  @Test
  def drop(): Unit = {
    users.upsertGroup(new Group(GroupA)).get
    users.upsertGroup(new Group(GroupB)).get
    waitUntilGroupPresent(GroupA)
    waitUntilGroupPresent(GroupB)
    users.dropGroup(GroupB).get
    waitUntilGroupDropped(GroupB)
    val actualNames = users.getAllGroups().get.map(_.name)
    assert(actualNames.contains(GroupA))
    assert(!actualNames.contains(GroupB))
  }

  @Test
  def emptyGroups() = {
    assert(users.getAllGroups().get.isEmpty)
  }

  @Test
  @IgnoreWhen(hasCapabilities = Array(Capabilities.COLLECTIONS))
  def createPreCheshireCat(): Unit = {
    create(SecurityAdmin)
  }

  @Test
  @IgnoreWhen(missesCapabilities = Array(Capabilities.COLLECTIONS))
  def createPostCheshireCat(): Unit = {
    create(SecurityAdminLocal)
  }

  def create(securityAdmin: Role): Unit = {
    val fakeLdapRef = "ou=Users"
    users
      .upsertGroup(
        new Group(GroupA).description("a").roles(ReadOnlyAdmin).ldapGroupReference(fakeLdapRef)
      )
      .get
    users
      .upsertGroup(
        new Group(GroupB).description("b").roles(ReadOnlyAdmin, BucketFullAccessWildcard)
      )
      .get
    waitUntilGroupPresent(GroupA)
    waitUntilGroupPresent(GroupB)

    assertEquals("a", users.getGroup(GroupA).get.description)
    assertEquals("b", users.getGroup(GroupB).get.description)

    assertEquals(Some(fakeLdapRef), users.getGroup(GroupA).get.ldapGroupReference)
    assertEquals(Option.empty, users.getGroup(GroupB).get.ldapGroupReference)

    assertEquals(Set(ReadOnlyAdmin), users.getGroup(GroupA).get.roles.toSet)
    assertEquals(
      Set(ReadOnlyAdmin, BucketFullAccessWildcard),
      users.getGroup(GroupB).get.roles.toSet
    )

    users
      .upsertUser(
        User(Username)
          .password("password")
          .roles(securityAdmin, BucketFullAccessWildcard)
          .groups(GroupA, GroupB)
      )
      .get

    waitUntilUserPresent(Username)

    var userMeta = users.getUser(Username, AuthDomain.Local).get

    assertEquals(Set(securityAdmin, BucketFullAccessWildcard), userMeta.user.roles.toSet)
    assertEquals(
      Set(securityAdmin, BucketFullAccessWildcard, ReadOnlyAdmin),
      userMeta.effectiveRoles.map(_.role).toSet
    )

    assert(userMeta.effectiveRoles.size == 3)
    val r1 = userMeta.effectiveRoles.find(_.role.name == securityAdmin.name).get
    val r2 = userMeta.effectiveRoles.find(_.role.name == "ro_admin").get
    val r3 = userMeta.effectiveRoles.find(_.role.name == "bucket_full_access").get

    assert(r1.origins.size == 1)
    assert(r1.origins.head.typ == "user")

    assert(r2.origins.size == 2)
    assert(r2.origins.exists(v => v.typ == "group" && v.name.contains("group-a")))
    assert(r2.origins.exists(v => v.typ == "group" && v.name.contains("group-b")))

    assert(r3.origins.size == 2)
    assert(r3.origins.exists(v => v.typ == "user"))
    assert(r3.origins.exists(v => v.typ == "group" && v.name.contains("group-b")))

    users.upsertGroup(users.getGroup(GroupA).get.roles(securityAdmin)).get
    users.upsertGroup(users.getGroup(GroupB).get.roles(securityAdmin)).get

    Util.waitUntilCondition(() => {
      users.getGroup(GroupA) match {
        case Success(value) => value.roles.size == 1
        case _              => false
      }
    })
    Util.waitUntilCondition(() => {
      users.getGroup(GroupB) match {
        case Success(value) => value.roles.size == 1
        case _              => false
      }
    })

    userMeta = users.getUser(Username, AuthDomain.Local).get
    assertEquals(
      Set(securityAdmin, BucketFullAccessWildcard),
      userMeta.effectiveRoles.map(_.role).toSet
    )
  }

  @Test
  def dropAbsentGroup(): Unit = {
    val name = "doesnotexist"
    val e    = assertThrows(classOf[GroupNotFoundException], () => users.dropGroup(name).get)
    assertEquals(name, e.groupName)
  }

  @Test
  def removeUserFromAllGroups(): Unit = {
    // exercise the special-case code for upserting an empty group list.
    users.upsertGroup(new Group(GroupA).roles(ReadOnlyAdmin)).get
    waitUntilGroupPresent(GroupA)
    users.upsertUser(User(Username).password("password").groups(GroupA)).get
    waitUntilUserPresent(Username)

    var userMeta = users.getUser(Username, AuthDomain.Local).get
    assertEquals(Vector(ReadOnlyAdmin), userMeta.effectiveRoles.map(_.role))

    users.upsertUser(userMeta.user.groups()).get

    Util.waitUntilCondition(() => {
      users.getUser(Username) match {
        case Success(value) => value.groups.isEmpty
        case _              => false
      }
    })

    userMeta = users.getUser(Username, AuthDomain.Local).get
    assert(userMeta.effectiveRoles.isEmpty)
  }

  @Test
  def parseGroup(): Unit = {
    val raw =
      """{"id":"group-a","roles":[{"role":"ro_admin"}],"ldap_group_ref":"ou=Users","description":"a"}"""
    val group = CouchbasePickler.read[Group](raw)
    assert(group.roles.size == 1)
    assert(group.ldapGroupReference.contains("ou=Users"))
    assert(group.description == "a")
  }
}
