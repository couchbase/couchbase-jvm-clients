package com.couchbase.client.scala.manager.user

import java.time.Instant

import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty
import com.couchbase.client.scala.util.CouchbasePickler
import upickle.default.{macroRW, ReadWriter => RW}

/** Models a Couchbase user.
  *
  * @param username    the user's username
  * @param displayName the user's display name
  * @param groups      any groups that the user belongs to
  * @param roles       any roles directly assigned to the user (not those inherited through groups)
  */
@Volatile
case class User(username: String,
                displayName: String = "",
                groups: Seq[String] = Seq(),
                private val _roles: Seq[Role] = Seq(),
                private[scala] val password: Option[String] = None) {

  /** Creates a copy of this User with the new username. */
  def username(username: String): User = {
    copy(username = username)
  }

  /** Creates a copy of this User with the new display name. */
  def displayName(displayName: String): User = {
    copy(displayName = displayName)
  }

  /** Creates a copy of this User with the new groups. */
  def groups(groups: String*): User = {
    copy(groups = groups)
  }

  /** Creates a copy of this User with the new roles. */
  def roles(newRoles: Role*): User = {
    copy(_roles = newRoles)
  }

  def roles: Seq[Role] = _roles

  /** Creates a copy of this User with the new password. */
  def password(password: String): User = {
    copy(password = Some(password))
  }
}


/** Associates a [[User]] with any derived properties, such as the effective roles inherited from groups.
  */
@Volatile
case class UserAndMetadata(@upickle.implicits.key("domain") domain: AuthDomain,
                           @upickle.implicits.key("id") username: String,
                           @upickle.implicits.key("name") displayName: String,
                           @upickle.implicits.key("roles") private[scala] val _effectiveRoles: Seq[RoleAndOrigins],
                           @upickle.implicits.key("password_change_date") _passwordChanged: Option[String],
                           @upickle.implicits.key("groups") groups: Seq[String],
                           @upickle.implicits.key("external_groups") externalGroups: Seq[String]) {
  /** Returns the roles assigned specifically to the user. Excludes roles that are
    * only inherited from groups.
    */
  def innateRoles: Seq[Role] = {
    _effectiveRoles.filter(_.innate).map(_.role)
  }

  /** Returns all of the user's roles, including roles inherited from groups.
    */
  def effectiveRoles: Seq[Role] = {
    _effectiveRoles.map(_.role)
  }

  def effectiveRolesAndOrigins: Seq[RoleAndOrigins] = {
    _effectiveRoles
  }

  def passwordChanged: Option[Instant] = {
    _passwordChanged.map(v => Instant.parse(v))
  }

  def user: User = {
    User(username,
      displayName,
      groups,
      innateRoles)
  }
}

object UserAndMetadata {
  implicit val rw: CouchbasePickler.ReadWriter[UserAndMetadata] = CouchbasePickler.macroRW
}