package com.couchbase.client.scala.env

import com.couchbase.client.core.env.Credentials

case class RoleBasedCredentials(username: String, password: String) extends Credentials {
  override def usernameForBucket(bucket: String): String = username
  override def passwordForBucket(bucket: String): String = password
}
