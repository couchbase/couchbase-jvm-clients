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

import com.couchbase.client.scala.env.{ClusterEnvironment, SeedNode, UsernameAndPassword}
import com.couchbase.client.test.{ClusterAwareIntegrationTest, Services}
import org.junit.jupiter.api.Timeout

import scala.collection.JavaConverters._

/**
  * Extends the {@link ClusterAwareIntegrationTest} with scala-client specific code.
  *
  * @since 3.0.0
  */
object ScalaIntegrationTest {

}

@Timeout(value = 1, unit = TimeUnit.MINUTES) // Safety timer so tests can't block CI executors
trait ScalaIntegrationTest extends ClusterAwareIntegrationTest {
  /**
    * Creates a {@link ClusterEnvironment.Builder} which already has the seed nodes and
    * credentials plugged and ready to use depending on the environment.
    *
    * @return the builder, ready to be further modified or used directly.
    */
  protected def environment: ClusterEnvironment.Builder = {
    val config = ClusterAwareIntegrationTest.config()
    val seeds = config.nodes.asScala.map(cfg =>
      SeedNode(cfg.hostname, Some(cfg.ports.get(Services.KV)), Some(cfg.ports.get(Services.MANAGER))))
      .toSet

    ClusterEnvironment
      .builder(UsernameAndPassword(config.adminUsername, config.adminPassword))
      .seedNodes(seeds)
  }
}
