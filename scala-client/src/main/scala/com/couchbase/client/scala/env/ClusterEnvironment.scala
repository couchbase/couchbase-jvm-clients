package com.couchbase.client.scala.env

import com.couchbase.client.core.env.{ConnectionStringPropertyLoader, CoreEnvironment, Credentials, RoleBasedCredentials}



class ClusterEnvironment(builder: ClusterEnvironment.Builder) extends CoreEnvironment(builder) {
  override protected def defaultAgentTitle(): String = "scala"
  override protected def agentPackage(): Package = classOf[ClusterEnvironment].getPackage


}

object ClusterEnvironment {
  class Builder(credentials: Credentials) extends CoreEnvironment.Builder[Builder](credentials) {
    override def build = new ClusterEnvironment(this)
  }

  def create(connectionString: String, username: String, password: String) = {
    new ClusterEnvironment(new Builder(new RoleBasedCredentials(username, password)))
  }

  def create(connectionString: String, credentials: Credentials): ClusterEnvironment = {
    new ClusterEnvironment(new Builder(credentials).load(new ConnectionStringPropertyLoader(connectionString)))
  }

  def builder(connectionString: String, username: String, password: String): ClusterEnvironment.Builder = {
    val credentials = new RoleBasedCredentials(username, password)
    new Builder(credentials).load(new ConnectionStringPropertyLoader(connectionString))
  }


  def builder(connectionString: String, credentials: Credentials): ClusterEnvironment.Builder = {
    new Builder(credentials).load(new ConnectionStringPropertyLoader(connectionString))
  }

  def builder(credentials: Credentials): ClusterEnvironment.Builder = {
    new Builder(credentials)
  }
}
