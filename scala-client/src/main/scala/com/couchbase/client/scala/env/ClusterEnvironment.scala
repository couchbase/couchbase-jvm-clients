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

package com.couchbase.client.scala.env

import java.util.concurrent.{Executors, ThreadFactory}

import com.couchbase.client.core.env.{
  ConnectionStringPropertyLoader, CoreEnvironment, Credentials,
  RoleBasedCredentials
}
import reactor.core.scala.scheduler.ExecutionContextScheduler
import reactor.core.scheduler.Scheduler

import scala.concurrent.ExecutionContext
import scala.util.Try


/** Functions to create a ClusterEnvironment, which provides configuration options for connecting to a Couchbase
  * cluster.
  *
  * This environment also contains long-lived resources such as a thread-pool, so the application should take care to
  * only create one of these.  The same environment can be shared by multiple cluster connections.
  */
object ClusterEnvironment {
  // Create the thread pool that will be used for all Future throughout the SDK.
  private val numCores = Runtime.getRuntime.availableProcessors
  // TODO move thread pool into local cluster env
  private[scala] val threadPool = Executors.newFixedThreadPool(numCores, new ThreadFactory {
    override def newThread(runnable: Runnable): Thread = {
      val thread = new Thread(runnable)
      // Make it a daemon thread so it doesn't block app exit
      thread.setDaemon(true)
      thread.setName("cb-comps-" + thread.getId)
      thread
    }
  })
  private[scala] implicit val ec = ExecutionContext.fromExecutor(threadPool)
  private[scala] val defaultScheduler = ExecutionContextScheduler(ec)

  // TODO want to re-implement CoreEnvironment.Builder in Scala
  class Builder(credentials: Credentials) extends CoreEnvironment.Builder[Builder](credentials) {
    override def build = new ClusterEnvironment(scheduler(defaultScheduler))

    // None of the builder methods throw currently on invalid config (e.g. a minimum compression size < 0).  If they do,
    // the exception will instead be stored and raised in this Try.
    // TODO some do throw and need handling, see BucketConfigParser (e.g. bad connection string)
    def buildSafe = Try(build)
  }

  /** Creates a ClusterEnvironment to connect to a Couchbase cluster with a username and password as credentials.
    *
    * All other configuration options are left at their default.  Use one of the
    * [[ClusterEnvironment.builder]] overloads to setup a more customized environment.
    *
    * @param connectionString connection string used to locate the Couchbase cluster.
    * @param username         the name of a user with appropriate permissions on the cluster.
    * @param password         the password of a user with appropriate permissions on the cluster.
    *
    * @return a constructed `ClusterEnvironment`
    */
  def create(connectionString: String, username: String, password: String) = {
    new ClusterEnvironment(new Builder(new RoleBasedCredentials(username, password)).load(new
        ConnectionStringPropertyLoader(connectionString)))
  }

  /** Creates a ClusterEnvironment to connect to a Couchbase cluster with custom [[Credentials]].
    *
    * All other configuration options are left at their default.  Use one of the
    * [[ClusterEnvironment.builder]] overloads to setup a more customized environment.
    *
    * @param connectionString connection string used to locate the Couchbase cluster.
    * @param credentials      custom credentials used when connecting to the cluster.
    *
    * @return a constructed `ClusterEnvironment`
    */
  def create(connectionString: String, credentials: Credentials): ClusterEnvironment = {
    new ClusterEnvironment(new Builder(credentials).load(new ConnectionStringPropertyLoader(connectionString)))
  }

  /** Creates a `ClusterEnvironment.Builder` setup to connect to a Couchbase cluster with a username and password as
    * credentials.
    *
    * All other configuration options can be customzed with the methods on the returned `ClusterEnvironment.Builder`.
    * Use `build` to finalize the builder into a `ClusterEnvironment`, ready for use with the methods in
    * [[com.couchbase.client.scala.Cluster]].
    *
    * @param connectionString connection string used to locate the Couchbase cluster.
    * @param username         the name of a user with appropriate permissions on the cluster.
    * @param password         the password of a user with appropriate permissions on the cluster.
    *
    * @return a `ClusterEnvironment.Builder`
    */
  def builder(connectionString: String, username: String, password: String): ClusterEnvironment.Builder = {
    val credentials = new RoleBasedCredentials(username, password)
    new Builder(credentials).load(new ConnectionStringPropertyLoader(connectionString))
  }


  /** Creates a `ClusterEnvironment.Builder` setup to connect to a Couchbase cluster with custom [[Credentials]].
    *
    * All other configuration options can be customzed with the methods on the returned `ClusterEnvironment.Builder`.
    * Use `build` to finalize the builder into a `ClusterEnvironment`, ready for use with the methods in
    * [[com.couchbase.client.scala.Cluster]].
    *
    * @param connectionString connection string used to locate the Couchbase cluster.
    * @param credentials      custom credentials used when connecting to the cluster.
    *
    * @return a `ClusterEnvironment.Builder`
    */
  def builder(connectionString: String, credentials: Credentials): ClusterEnvironment.Builder = {
    new Builder(credentials).load(new ConnectionStringPropertyLoader(connectionString))
  }

  /** Creates a `ClusterEnvironment.Builder` setup to connect with custom [[Credentials]].
    *
    * All other configuration options can be customzed with the methods on the returned `ClusterEnvironment.Builder`.
    * Use `build` to finalize the builder into a `ClusterEnvironment`, ready for use with the methods in
    * [[com.couchbase.client.scala.Cluster]].
    *
    * Note that this overload does not provide the details of any nodes in the Couchbase cluster, so at least the
    * `seedNodes` option must be provided.
    *
    * @param credentials      custom credentials used when connecting to the cluster.
    *
    * @return a ClusterEnvironment.Builder`
    */
  def builder(credentials: Credentials): ClusterEnvironment.Builder = {
    new Builder(credentials)
  }
}

class ClusterEnvironment(builder: ClusterEnvironment.Builder) extends CoreEnvironment(builder) {
  override protected def defaultAgentTitle(): String = "scala"

  override protected def agentPackage(): Package = classOf[ClusterEnvironment].getPackage
}
