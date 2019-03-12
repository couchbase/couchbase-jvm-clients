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

import com.couchbase.client.core.env.{ConnectionStringPropertyLoader, CoreEnvironment, Credentials, RoleBasedCredentials}
import reactor.core.scala.scheduler.ExecutionContextScheduler
import reactor.core.scheduler.Scheduler

import scala.concurrent.ExecutionContext
import scala.util.Try


// TODO ScalaDocs
class ClusterEnvironment(builder: ClusterEnvironment.Builder) extends CoreEnvironment(builder) {
  override protected def defaultAgentTitle(): String = "scala"
  override protected def agentPackage(): Package = classOf[ClusterEnvironment].getPackage
}

object ClusterEnvironment {
  // Create the thread pool that will be used for all Future throughout the SDK.
  private val numCores = Runtime.getRuntime.availableProcessors
  private val threadPool = Executors.newFixedThreadPool(numCores, new ThreadFactory {
    override def newThread(runnable: Runnable): Thread = {
      val thread = new Thread(runnable)
      thread.setName("cb-comps-" + thread.getId)
      thread
    }
  })
  private[scala] implicit val ec = ExecutionContext.fromExecutor(threadPool)
  private val defaultScheduler = ExecutionContextScheduler(ec)

  class Builder(credentials: Credentials) extends CoreEnvironment.Builder[Builder](credentials) {
    override def build = new ClusterEnvironment(scheduler(defaultScheduler))

    // None of the builder methods throw currently on invalid config (e.g. a minimum compression size < 0).  If they do,
    // the exception will instead be stored and raised in this Try.
    // TODO some do throw and need handling, see BucketConfigParser (e.g. bad connection string)
    def buildSafe = Try(build)
  }

  def create(connectionString: String, username: String, password: String) = {
    new ClusterEnvironment(new Builder(new RoleBasedCredentials(username, password)).load(new ConnectionStringPropertyLoader(connectionString)))
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
