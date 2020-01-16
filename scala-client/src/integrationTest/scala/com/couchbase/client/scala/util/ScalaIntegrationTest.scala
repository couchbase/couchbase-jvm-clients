package com.couchbase.client.scala.util

/*
 * Copyright (c) 2018 Couchbase, Inc.
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

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.env.Authenticator
import com.couchbase.client.scala.{Cluster, ClusterOptions, env}
import com.couchbase.client.scala.env.{ClusterEnvironment, PasswordAuthenticator, SeedNode}
import com.couchbase.client.test.{
  ClusterAwareIntegrationTest,
  Services,
  TestClusterConfig,
  TestNodeConfig
}
import org.junit.jupiter.api.Timeout

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._

/**
  * Extends the {@link ClusterAwareIntegrationTest} with scala-client specific code.
  *
  * @since 3.0.0
  */
object ScalaIntegrationTest {}

@Timeout(value = 1, unit = TimeUnit.MINUTES) // Safety timer so tests can't block CI executors
trait ScalaIntegrationTest extends ClusterAwareIntegrationTest {

  /**
    * Creates a {@link ClusterEnvironment.Builder} which already has the seed nodes and
    * credentials plugged and ready to use depending on the environment.
    *
    * @return the builder, ready to be further modified or used directly.
    */
  protected def environment: ClusterEnvironment.Builder = {
    ClusterEnvironment.builder
  }

  /**
    * Creates the right connection string out of the seed nodes in the config.
    *
    * @return the connection string to connect.
    */
  protected def connectionString: String = {
    val strings = seedNodes.map((s: SeedNode) => {
      s.kvPort match {
        case Some(kvPort) => s.address + ":" + kvPort
        case _            => s.address
      }
    })

    strings.mkString(",")
  }

  private def seedNodes: Set[SeedNode] = {
    config.nodes.asScala
      .map((cfg: TestNodeConfig) => {
        val kvPort   = Some(cfg.ports.get(Services.KV).toInt)
        val httpPort = Some(cfg.ports.get(Services.MANAGER).toInt)

        SeedNode(cfg.hostname, kvPort, httpPort)
      })
      .toSet
  }

  protected def config: TestClusterConfig = {
    ClusterAwareIntegrationTest.config()
  }

  /**
    * Returns the pre-set cluster options with the environment and authenticator configured.
    *
    * @return the cluster options ready to be used.
    */
  protected def clusterOptions: ClusterOptions = {
    ClusterOptions(authenticator, environment.build.toOption)
  }

  protected def authenticator: Authenticator = {
    PasswordAuthenticator(config.adminUsername, config.adminPassword)
  }

  protected def connectToCluster(): Cluster = {
    val out = Cluster.connect(connectionString, clusterOptions).get
    out.waitUntilReady(10.seconds)
    out
  }
}
