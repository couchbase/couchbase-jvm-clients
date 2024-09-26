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

package com.couchbase.client.scala.util

import com.couchbase.client.scala.env.{
  ClusterEnvironment,
  PasswordAuthenticator,
  SecurityConfig,
  SeedNode
}
import com.couchbase.client.scala.{Cluster, ClusterOptions}
import com.couchbase.client.test.{
  ClusterAwareIntegrationTest,
  Services,
  TestClusterConfig,
  TestNodeConfig
}
import org.junit.jupiter.api.Timeout
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/**
  * Extends the {@link ClusterAwareIntegrationTest} with scala-client specific code.
  *
  * @since 3.0.0
  */
object ScalaIntegrationTest {
  val Logger = LoggerFactory.getLogger(classOf[ScalaIntegrationTest])
}

// Temporarily increased timeout to (possibly) workaround MB-37011 when Developer Preview enabled
@Timeout(value = 10, unit = TimeUnit.MINUTES) // Safety timer so tests can't block CI executors
trait ScalaIntegrationTest extends ClusterAwareIntegrationTest {

  // Timeouts seen on CI with values of 30 seconds.
  val WaitUntilReadyDefault = Duration(60, TimeUnit.SECONDS)

  /**
    * Creates a {@link ClusterEnvironment.Builder} which already has the seed nodes and
    * credentials plugged and ready to use depending on the environment.
    *
    * @return the builder, ready to be further modified or used directly.
    */
  protected def environment: ClusterEnvironment.Builder = {
    var builder = ClusterEnvironment.builder
    if (config.runWithTLS()) {
      builder = ClusterEnvironment.builder.securityConfig(
        SecurityConfig()
          .enableTls(true)
          .trustCertificates(
            JavaConverters
              .asScalaIteratorConverter(config.clusterCerts().get().iterator())
              .asScala
              .toSeq
          )
      )
    }
    builder
  }

  /**
    * Creates the right connection string out of the seed nodes in the config.
    *
    * @return the connection string to connect.
    */
  protected def connectionString: String = {
    val strings = seedNodes.map((s: SeedNode) => {
      s.protostellarPort match {
        case Some(psPort) => "protostellar://" + s.address + ":" + psPort
        case _ =>
          s.kvPort match {
            case Some(kvPort) => s.address + ":" + kvPort
            case _            => s.address
          }
      }
    })

    strings.mkString(",")
  }

  protected def seedNodes =
    config.nodes.asScala
      .map((cfg: TestNodeConfig) => {
        if (cfg.protostellarPort.isPresent) {
          SeedNode(cfg.hostname, None, None, cfg.protostellarPort.asScala.map(v => v.toInt))
        } else {
          var kvPort   = Some(cfg.ports.get(Services.KV).toInt)
          var httpPort = Some(cfg.ports.get(Services.MANAGER).toInt)

          if (config.runWithTLS()) {
            kvPort = Some(cfg.ports.get(Services.KV_TLS).toInt)
            httpPort = Some(cfg.ports.get(Services.MANAGER_TLS).toInt)
          }

          SeedNode(cfg.hostname, kvPort, httpPort)
        }
      })
      .toSet

  protected def config: TestClusterConfig = ClusterAwareIntegrationTest.config()

  /**
    * Returns the pre-set cluster options with the environment and authenticator configured.
    *
    * @return the cluster options ready to be used.
    */
  protected def clusterOptions =
    ClusterOptions(authenticator, environment.build.toOption)

  protected def authenticator =
    PasswordAuthenticator(config.adminUsername, config.adminPassword)

  protected def connectToCluster(): Cluster = {
    val out = Cluster.connect(connectionString, clusterOptions).get
    out.waitUntilReady(WaitUntilReadyDefault)
    out
  }
}
