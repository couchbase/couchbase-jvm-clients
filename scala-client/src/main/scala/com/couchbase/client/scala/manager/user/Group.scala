package com.couchbase.client.scala.manager.user

import com.couchbase.client.scala.util.CouchbasePickler

/** Defines a set of roles that may be inherited by users.
  *
  * @param name        the groups' name
  * @param description the group's description
  * @param roles       any roles associated with the group
  */
case class Group(@upickle.implicits.key("id") name: String,
                 description: String = "",
                 roles: Seq[Role] = Seq(),
                 @upickle.implicits.key("ldap_group_ref") ldapGroupReference: Option[String] = None) {

  /** Creates a copy of this Group with a new name. */
  def name(name: String): Group = {
    copy(name = name)
  }

  /** Creates a copy of this Group with a new description. */
  def description(description: String): Group = {
    copy(description = description)
  }

  /** Creates a copy of this Group with a new name. */
  def roles(roles: Role*): Group = {
    copy(roles = roles)
  }

  /** Creates a copy of this Group with a new ldapGroupReference. */
  def ldapGroupReference(ldapGroupReference: String): Group = {
    copy(ldapGroupReference = Some(ldapGroupReference))
  }
}

object Group {
  implicit val rw: CouchbasePickler.ReadWriter[Group] = CouchbasePickler.macroRW
}