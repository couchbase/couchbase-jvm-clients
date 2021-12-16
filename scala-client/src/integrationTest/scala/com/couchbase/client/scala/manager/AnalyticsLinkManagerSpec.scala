/*
 * Copyright (c) 2021 Couchbase, Inc.
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
package com.couchbase.client.scala.manager

import com.couchbase.client.core.error._
import com.couchbase.client.scala.Cluster
import com.couchbase.client.scala.manager.analytics.AnalyticsLink.{
  CouchbaseRemoteAnalyticsLink,
  S3ExternalAnalyticsLink
}
import com.couchbase.client.scala.manager.analytics._
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.test.{Capabilities, IgnoreWhen}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._
import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.scala.manager.analytics.ReactiveAnalyticsIndexManager.quoteDataverse

import scala.util.{Failure, Success}

@TestInstance(Lifecycle.PER_CLASS)
// Use COLLECTIONS as a proxy for 7.0. Analytics dataset management is only supported from 7.0.
@IgnoreWhen(missesCapabilities = Array(Capabilities.ANALYTICS, Capabilities.COLLECTIONS))
class AnalyticsLinkManagerSpec extends ScalaIntegrationTest {
  private var cluster: Cluster                 = _
  private var analytics: AnalyticsIndexManager = _
  private val DataverseName                    = "inventory"

  @BeforeAll
  def setup(): Unit = {
    cluster = connectToCluster()
    analytics = cluster.analyticsIndexes
  }

  @AfterAll
  def tearDown(): Unit = {
    analytics.dropDataverse(DataverseName) // ignore result
    cluster.disconnect()
  }

  @BeforeEach
  def beforeEach(): Unit = {
    val result = analytics.dropDataverse(DataverseName)

    result match {
      case Success(_)                               =>
      case Failure(err: DataverseNotFoundException) =>
      case Failure(err) =>
        assert(false)
    }
  }

  /** Manual testing setup:
    * Create a remote cluster with analytics. Install travel-sample.
    * Change remoteHostname to point to this.
    */
  @Disabled(
    "Due to the complexity of testing analytics remote links, this needs to be done manually"
  )
  @Test
  def manualCouchbaseRemoteAnalyticsLink(): Unit = {
    val remoteHostname = "172.23.111.128"

    analytics.createDataverse("inventory").get

    // Cannot use `travel-sample`.inventory
    analytics
      .createLink(
        CouchbaseRemoteAnalyticsLink(
          "inventory",
          "remoteInventory",
          remoteHostname,
          AnalyticsEncryptionLevel.None(UsernameAndPassword("Administrator", "password"))
        )
      )
      .get

    try {
      val links = analytics.getLinks().get
      assert(links.size == 1)
      assert(
        analytics.getLinks(linkType = Some(AnalyticsLinkType.CouchbaseRemote)).get.size == 1
      )
      assert(analytics.getLinks(dataverse = Some("inventory")).get.size == 1)
      assert(
        analytics
          .getLinks(dataverse = Some("inventory"), name = Some("remoteInventory"))
          .get
          .size == 1
      )
      assert(
        analytics
          .getLinks(dataverse = Some("inventory"), name = Some("remoteInventory"))
          .get
          .size == 1
      )
      assert(
        analytics
          .getLinks(dataverse = Some("inventory"), name = Some("doesNotExist"))
          .get
          .isEmpty
      )
      assert(analytics.getLinks(linkType = Some(AnalyticsLinkType.S3External)).get.isEmpty)

      assert(links.head.linkType == AnalyticsLinkType.CouchbaseRemote)
      links.head match {
        case l: CouchbaseRemoteAnalyticsLink =>
          assert(l.dataverse == "inventory")
          assert(l.name == "remoteInventory")
          assert(
            l.encryption == AnalyticsEncryptionLevel.None(UsernameAndPassword("Administrator", ""))
          )
        case _ => assert(false)
      }
    } finally {
      analytics.dropLink("remoteInventory", DataverseName) // ignore result
      assert(analytics.getLinks().get.isEmpty)
    }
  }

  /** Doesn't test the server S3 functionality, simply that the link is created. */
  @Test
  def s3ExternalLink(): Unit = {
    analytics.createDataverse(DataverseName).get
    val linkName = "s3Inventory"

    // Cannot use `travel-sample`.inventory
    val origLink = S3ExternalAnalyticsLink(
      DataverseName,
      linkName,
      "s3AccessKeyId",
      "s3SecretAccessKey",
      "s3SessionToken",
      "us-east-1",
      "s3ServiceEndpoint"
    )
    analytics.createLink(origLink).get

    try {
      val links = analytics.getLinks().get
      assert(links.size == 1)
      assert(analytics.getLinks(linkType = Some(AnalyticsLinkType.S3External)).get.size == 1)
      assert(analytics.getLinks(dataverse = Some(DataverseName)).get.size == 1)
      assert(
        analytics.getLinks(dataverse = Some(DataverseName), name = Some(linkName)).get.size == 1
      )
      assert(
        analytics.getLinks(dataverse = Some(DataverseName), name = Some(linkName)).get.size == 1
      )
      assert(
        analytics
          .getLinks(dataverse = Some(DataverseName), name = Some("doesNotExist"))
          .get
          .isEmpty
      )
      assert(analytics.getLinks(linkType = Some(AnalyticsLinkType.CouchbaseRemote)).get.isEmpty)

      assert(links.head.linkType == AnalyticsLinkType.S3External)
      links.head match {
        case l: S3ExternalAnalyticsLink =>
          assert(l.dataverse == origLink.dataverse)
          assert(l.name == origLink.name)
          assert(l.accessKeyID == origLink.accessKeyID)
          assert(l.region == origLink.region)
          assert(l.secretAccessKey == "")
          assert(l.sessionToken == "")
        case _ => assert(false)
      }
    } finally {
      analytics.dropLink(origLink.name, DataverseName) // ignore result
      assert(analytics.getLinks().get.isEmpty)
    }
  }

  @Test
  def createExistingLink(): Unit = {
    analytics.createDataverse(DataverseName).get
    val linkName = "s3Inventory"

    val origLink = S3ExternalAnalyticsLink(
      DataverseName,
      linkName,
      "s3AccessKeyId",
      "s3SecretAccessKey",
      "s3SessionToken",
      "us-east-1",
      "s3ServiceEndpoint"
    )
    analytics.createLink(origLink).get
    analytics.createLink(origLink) match {
      case Failure(err: LinkExistsException) =>
      case _                                 => assert(false)
    }
  }

  @Test
  def replaceNonExistentLink(): Unit = {
    analytics.createDataverse(DataverseName).get
    val linkName = "s3Inventory"

    val link = S3ExternalAnalyticsLink(
      DataverseName,
      "does not exist",
      "s3AccessKeyId",
      "s3SecretAccessKey",
      "s3SessionToken",
      "s3Region",
      "s3ServiceEndpoint"
    )
    analytics.replaceLink(link) match {
      case Failure(err: LinkNotFoundException) =>
      case _                                   => assert(false)
    }
  }

  @Test
  def getAllLinksOnlyNameSpecified(): Unit = {
    analytics.getLinks(name = Some("doesNotExist")) match {
      case Failure(e: InvalidArgumentException) =>
      case _                                    => assert(false)
    }
  }

  @Test
  def linkDoesNotExist(): Unit = {
    analytics.getLinks(dataverse = Some("doesNotExist")) match {
      case Failure(e: DataverseNotFoundException) =>
      case _                                      => assert(false)
    }

    analytics.getLinks(dataverse = Some("doesNotExist"), name = Some("doesNotExist")) match {
      case Failure(e: DataverseNotFoundException) =>
      case _                                      => assert(false)
    }
  }

  @Test
  def canQuoteCompoundDataverseNames() {
    assert("`foo`" == quoteDataverse("foo").get)
    assert("`foo`.`bar`.`zot`" == quoteDataverse("foo/bar/zot").get)
    assert("`foo`.`bar`.`zot`" == quoteDataverse("foo/bar", "zot").get)
  }

  @Test
  def convertsSlashesOnlyinDataverseName() {
    assert("`foo`.`bar/zot`" == quoteDataverse("foo", "bar/zot").get)
  }
}
