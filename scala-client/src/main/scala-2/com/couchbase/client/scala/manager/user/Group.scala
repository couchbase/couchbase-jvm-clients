/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.scala.manager.user

import com.couchbase.client.scala.util.CouchbasePickler
import scala.util.Try

/** Defines a set of roles that may be inherited by users.
  *
  * @param name        the groups' name
  * @param description the group's description
  * @param roles       any roles associated with the group
  */
case class Group(
    @upickle.implicits.key("id") name: String,
    description: String = "",
    roles: Seq[Role] = Seq(),
    @upickle.implicits.key("ldap_group_ref") ldapGroupReference: Option[String] = None
) {

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
  implicit val rw: CouchbasePickler.ReadWriter[Group] = CouchbasePickler
    .readwriter[ujson.Obj]
    .bimap[Group](
      (g: Group) => {
        val obj = ujson.Obj(
          "id"          -> ujson.Str(g.name),
          "description" -> ujson.Str(g.description),
          "roles"       -> CouchbasePickler.writeJs(g.roles)
        )
        g.ldapGroupReference.foreach(l => obj.update("ldap_group_ref", l))
        obj
      },
      (json: ujson.Obj) => {
        val name         = Try(json("id").str).getOrElse("")
        val description  = Try(json("description").str).getOrElse("")
        val roles        = Try(CouchbasePickler.read[Seq[Role]](json("roles"))).getOrElse(Seq.empty)
        val ldapGroupRef = Try(json("ldap_group_ref").str).toOption
        Group(name, description, roles, ldapGroupRef)
      }
    )
}
