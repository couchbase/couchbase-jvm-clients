package com.couchbase.client.scala.env

import com.couchbase.client.core.env.CoreEnvironment.Builder
import com.couchbase.client.core.env.{CoreEnvironment, Credentials}



class ClusterEnvironment(builder: Builder) extends CoreEnvironment(builder) {


}

class Builder(credentials: Credentials) extends CoreEnvironment.Builder[Builder](credentials) {
}

object ClusterEnvironment {
  def create(connectionString: String, username: String, password: String) = {
    new ClusterEnvironment(new Builder(RoleBasedCredentials(username, password)))
  }

  // TODO MVP implement other credentials
}
