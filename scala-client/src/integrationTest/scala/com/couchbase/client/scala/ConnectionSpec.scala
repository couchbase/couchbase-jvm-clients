package com.couchbase.client.scala

/*
 * Copyright (c) 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"));
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

import com.couchbase.client.scala.env.{ClusterEnvironment, SecurityConfig}
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.kv.UpsertOptions
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.test.{ClusterType, IgnoreWhen}
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import org.junit.Assert.fail
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{Disabled, Test, TestInstance}

import java.nio.file.Path
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

@TestInstance(Lifecycle.PER_CLASS)
class ConnectionSpec extends ScalaIntegrationTest {

  @Test
  def performsKeyValueIgnoringServerCert(): Unit = {
    val env = ClusterEnvironment.builder
      .securityConfig(SecurityConfig().trustManagerFactory(InsecureTrustManagerFactory.INSTANCE))
      .build
      .get

    val cluster = Cluster.connect(connectionString, ClusterOptions(authenticator, Some(env))).get

    cluster
      .diagnostics()
      .map(result => System.out.println(result))
    val id = TestUtils.docId()
    cluster.bucket(config.bucketname()).defaultCollection.upsert(id, JsonObject.create).get
  }

  @IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
  @Test
  def performsKeyValueWithServerCert(): Unit = {
    if (!config.clusterCert.isPresent) fail("Cluster Certificate must be present for this test!")

    val env = ClusterEnvironment.builder
      .securityConfig(
        SecurityConfig()
          .enableTls(true)
          .trustCertificates(Seq(config.clusterCert().get()))
      )
      .build
      .get

    val cluster = Cluster
      .connect(seedNodes.map(_.address).mkString(","), ClusterOptions(authenticator, Some(env)))
      .get

    val id = TestUtils.docId()
    cluster
      .bucket(config.bucketname())
      .defaultCollection
      .upsert(id, JsonObject.create, UpsertOptions().timeout(Duration.create(20, TimeUnit.SECONDS)))
      .get
  }

//  @Disabled("Provided for manual testing")
//  @Test
//  def performsKeyValueWithServerCertManual(): Unit = {
//    val env = ClusterEnvironment.builder
//      .securityConfig(SecurityConfig()
//        .enableTls(true)
//        .trustCertificate(Path.of("/path/to/cluster.cert")))
//      .build.get
//
//    val cluster = Cluster.connect(seedNodes.map(_.address).mkString(","), ClusterOptions(authenticator, Some(env))).get
//
//    val id = TestUtils.docId()
//    cluster.bucket(config.bucketname()).defaultCollection.upsert(id, JsonObject.create).get
//  }
}
