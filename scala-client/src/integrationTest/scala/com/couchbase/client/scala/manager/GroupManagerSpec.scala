package com.couchbase.client.scala.manager

import com.couchbase.client.scala.manager.user.{UserNotFoundException, _}
import com.couchbase.client.scala.util.{CouchbasePickler, ScalaIntegrationTest}
import com.couchbase.client.scala.{Cluster, Collection}
import com.couchbase.client.test._
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._
import reactor.core.scala.publisher.SMono

@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
class GroupManagerSpec extends ScalaIntegrationTest {
  private var cluster: Cluster           = _
  private var users: ReactiveUserManager = _
  private var coll: Collection           = _

  private val Username                 = "integration-test-user"
  private val GroupA                   = "group-a"
  private val GroupB                   = "group-b"
  private val SecurityAdmin            = Role("security_admin")
  private val ReadOnlyAdmin            = Role("ro_admin")
  private val BucketFullAccessWildcard = Role("bucket_full_access", Some("*"))

  @BeforeAll
  def setup(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection
    users = cluster.reactive.users
  }

  @AfterAll
  def tearDown(): Unit = {
    cluster.disconnect()
  }

  private def dropUserQuietly(name: String): Unit = {
    users
      .dropUser(name)
      .onErrorResume(err => {
        if (err.isInstanceOf[UserNotFoundException]) SMono.empty
        else SMono.raiseError(err)
      })
      .block()
  }

  private def dropGroupQuietly(groupName: String): Unit = {
    users
      .dropGroup(groupName)
      .onErrorResume(err => {
        if (err.isInstanceOf[GroupNotFoundException]) SMono.empty
        else SMono.raiseError(err)
      })
      .block()
  }

  private def assertGroupAbsent(groupName: String): Unit = {
    val allUsers = users.getAllGroups().collectSeq().block()
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
    users.upsertGroup(new Group(GroupA)).block()
    users.upsertGroup(new Group(GroupB)).block()
    val actualNames = users.getAllGroups().collectSeq().block().map(_.name)
    assert(actualNames.contains(GroupA))
    assert(actualNames.contains(GroupB))
  }

  @Test
  def drop(): Unit = {
    users.upsertGroup(new Group(GroupA)).block()
    users.upsertGroup(new Group(GroupB)).block()
    users.dropGroup(GroupB).block()
    val actualNames = users.getAllGroups().collectSeq().block().map(_.name)
    assert(actualNames.contains(GroupA))
    assert(!actualNames.contains(GroupB))
  }

  @Test
  def create(): Unit = {
    val fakeLdapRef = "ou=Users"
    users
      .upsertGroup(
        new Group(GroupA).description("a").roles(ReadOnlyAdmin).ldapGroupReference(fakeLdapRef)
      )
      .block()
    users
      .upsertGroup(
        new Group(GroupB).description("b").roles(ReadOnlyAdmin, BucketFullAccessWildcard)
      )
      .block()

    assertEquals("a", users.getGroup(GroupA).block().description)
    assertEquals("b", users.getGroup(GroupB).block().description)

    assertEquals(Some(fakeLdapRef), users.getGroup(GroupA).block().ldapGroupReference)
    assertEquals(Option.empty, users.getGroup(GroupB).block().ldapGroupReference)

    assertEquals(Set(ReadOnlyAdmin), users.getGroup(GroupA).block().roles.toSet)
    assertEquals(
      Set(ReadOnlyAdmin, BucketFullAccessWildcard),
      users.getGroup(GroupB).block().roles.toSet
    )

    users
      .upsertUser(
        User(Username)
          .password("password")
          .roles(SecurityAdmin, BucketFullAccessWildcard)
          .groups(GroupA, GroupB)
      )
      .block()

    var userMeta = users.getUser(Username, AuthDomain.Local).block()

    assertEquals(Set(SecurityAdmin, BucketFullAccessWildcard), userMeta.user.roles.toSet)
    assertEquals(
      Set(SecurityAdmin, BucketFullAccessWildcard, ReadOnlyAdmin),
      userMeta.effectiveRoles.toSet
    )

    assert(userMeta.effectiveRolesAndOrigins.size == 3)
    val r1 = userMeta.effectiveRolesAndOrigins.find(_.role.name == "security_admin").get
    val r2 = userMeta.effectiveRolesAndOrigins.find(_.role.name == "ro_admin").get
    val r3 = userMeta.effectiveRolesAndOrigins.find(_.role.name == "bucket_full_access").get

    assert(r1.origins.size == 1)
    assert(r1.origins.head.typ == "user")

    assert(r2.origins.size == 2)
    assert(r2.origins.exists(v => v.typ == "group" && v.name.contains("group-a")))
    assert(r2.origins.exists(v => v.typ == "group" && v.name.contains("group-b")))

    assert(r3.origins.size == 2)
    assert(r3.origins.exists(v => v.typ == "user"))
    assert(r3.origins.exists(v => v.typ == "group" && v.name.contains("group-b")))

    users.upsertGroup(users.getGroup(GroupA).block().roles(SecurityAdmin)).block()
    users.upsertGroup(users.getGroup(GroupB).block().roles(SecurityAdmin)).block()

    userMeta = users.getUser(Username, AuthDomain.Local).block()
    assertEquals(Set(SecurityAdmin, BucketFullAccessWildcard), userMeta.effectiveRoles.toSet)
  }

  @Test
  def dropAbsentGroup(): Unit = {
    val name = "doesnotexist"
    val e    = assertThrows(classOf[GroupNotFoundException], () => users.dropGroup(name).block())
    assertEquals(name, e.groupName)
  }

  @Test
  def removeUserFromAllGroups(): Unit = {
    // exercise the special-case code for upserting an empty group list.
    users.upsertGroup(new Group(GroupA).roles(ReadOnlyAdmin)).block()
    users.upsertUser(User(Username).password("password").groups(GroupA)).block()

    var userMeta = users.getUser(Username, AuthDomain.Local).block()
    assertEquals(Seq(ReadOnlyAdmin), userMeta.effectiveRoles)

    users.upsertUser(userMeta.user.groups()).block()

    userMeta = users.getUser(Username, AuthDomain.Local).block()
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
