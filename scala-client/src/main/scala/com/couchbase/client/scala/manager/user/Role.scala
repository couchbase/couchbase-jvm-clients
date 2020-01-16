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

import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.scala.manager.user.AuthDomain.{External, Local}
import com.couchbase.client.scala.util.CouchbasePickler
import upickle.default.{macroRW, ReadWriter => RW}

import scala.util.Try

/** Identifies a specific permission possessed by a user.
  *
  * @param name   the role's name
  * @param bucket the bucket the role applies to, if available
  */
@Volatile
case class Role(name: String, bucket: Option[String] = None) {
  def format: String = {
    bucket match {
      case Some(b) => name + "[" + b + "]"
      case _       => name
    }
  }

  override def toString: String = format
}

object Role {
  // Get back "role":"admin" but want to store it as a Role, so custom serialization logic
  implicit val rw: CouchbasePickler.ReadWriter[Role] = CouchbasePickler
    .readwriter[ujson.Obj]
    .bimap[Role](
      (x: Role) => {
        val out = ujson.Obj("role" -> x.name)
        x.bucket.foreach(bucket => out("bucket") = bucket)
        out
      },
      (json: ujson.Obj) => {
        Role(
          Try(json("role").str).getOrElse("COULD NOT PARSE"),
          Try(Some(json("bucket_name").str)).getOrElse(None)
        )
      }
    )
}

/** Associates a role with its display name and description.
  *
  * @param role        the role itself
  * @param displayName the role's display name
  * @param description the role's description
  */
@Volatile
case class RoleAndDescription(role: Role, displayName: String, description: String)

object RoleAndDescription {
  // Get back "role":"admin" but want to store it as a Role, so custom serialization logic
  implicit val rw: CouchbasePickler.ReadWriter[RoleAndDescription] = CouchbasePickler
    .readwriter[ujson.Obj]
    .bimap[RoleAndDescription](
      (x: RoleAndDescription) => {
        ujson.Obj(
          "role" -> x.role.name,
          "name" -> x.displayName,
          "desc" -> x.description
        )
      },
      (json: ujson.Obj) => {
        RoleAndDescription(
          Role(Try(json("role").str).getOrElse("COULD NOT PARSE")),
          Try(json("name").str).getOrElse("COULD NOT PARSE"),
          Try(json("desc").str).getOrElse("COULD NOT PARSE")
        )
      }
    )
}

/** Indicates why the user has a specific role.
  *
  * If the type is “user” it means the role is assigned directly to the user. If the type is “group” it means the role
  * is inherited from the group identified by the “name” field.
  *
  * @param typ  the type - "user" or "group"
  * @param name only present if the type is "group"
  */
@Volatile
case class Origin(@upickle.implicits.key("type") typ: String, name: Option[String] = None) {

  override def toString: String = name.map(n => typ + ":" + n).getOrElse(typ)
}

object Origin {
  implicit val rw: CouchbasePickler.ReadWriter[Origin] = CouchbasePickler.macroRW
}

/** Associates a role with its origins.
  *
  * @param role    the role
  * @param origins the role's origins
  */
@Volatile
case class RoleAndOrigins(role: Role, origins: Seq[Origin]) {

  /** Returns true if this role is assigned specifically to the user (has origin "user"
    * as opposed to being inherited from a group).
    */
  def innate: Boolean = origins.exists(_.typ == "user")

  override def toString: String =
    role.toString + "<-" + origins.mkString("[", ",", "]") // match Java output
}

object RoleAndOrigins {
  // Get back "role":"admin" but want to store it as a Role, so custom serialization logic
  implicit val rw: CouchbasePickler.ReadWriter[RoleAndOrigins] = CouchbasePickler
    .readwriter[ujson.Obj]
    .bimap[RoleAndOrigins](
      (x: RoleAndOrigins) => {
        val json = ujson.Obj()
        json("role") = x.role.name
        json("origins") = CouchbasePickler.write(x.origins)
        json
      },
      (json: ujson.Obj) => {
        val origins                    = Try(json("origins")).toOption
        val bucketName: Option[String] = Try(json("bucket_name").str).toOption

        RoleAndOrigins(
          Role(json("role").str, bucketName),
          origins.map(v => CouchbasePickler.read[Seq[Origin]](v)).getOrElse(Seq())
        )
      }
    )
}
