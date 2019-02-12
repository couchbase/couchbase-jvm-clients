package com.couchbase.client.scala.env

import com.couchbase.client.core.env.{ConnectionStringPropertyLoader, CoreEnvironment, Credentials, RoleBasedCredentials}



class ClusterEnvironment(builder: Builder) extends CoreEnvironment(builder) {
  override protected def defaultAgentTitle(): String = "scala"
  override protected def agentPackage(): Package = classOf[ClusterEnvironment].getPackage
}

class Builder(credentials: Credentials) extends CoreEnvironment.Builder[Builder](credentials) {
}

object ClusterEnvironment {
  def create(connectionString: String, username: String, password: String) = {
    new ClusterEnvironment(new Builder(new RoleBasedCredentials(username, password)))
  }

  def create(connectionString: String, credentials: Credentials): ClusterEnvironment = {
    new ClusterEnvironment(new Builder(credentials).load(new ConnectionStringPropertyLoader(connectionString)))
  }
}
