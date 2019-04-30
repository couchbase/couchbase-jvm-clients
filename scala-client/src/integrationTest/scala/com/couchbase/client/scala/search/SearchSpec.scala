package com.couchbase.client.scala.search

import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection}
import com.couchbase.client.test.ClusterAwareIntegrationTest
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, TestInstance}

import scala.util.{Failure, Success}

//@TestInstance(Lifecycle.PER_CLASS)
//class SearchSpec extends ScalaIntegrationTest {
//
//  private var env: ClusterEnvironment = _
//  private var cluster: Cluster = _
//  private var coll: Collection = _
//
//  @BeforeAll
//  def beforeAll(): Unit = {
//    val config = ClusterAwareIntegrationTest.config()
//    env = environment.build
//    cluster = Cluster.connect(env)
//    val bucket = cluster.bucket(config.bucketname)
//    coll = bucket.defaultCollection
//  }
//
//  @AfterAll
//  def afterAll(): Unit = {
//    cluster.shutdown()
//    env.shutdown()
//  }
//}

//class SearchSpec extends FunSuite {
//
//  // TODO Commenting out for now.  Need a true solution to testing search
//  //  val cluster = Cluster.connect("localhost", "Administrator", "password")
//  //  val bucket = cluster.bucket("travel-sample")
//  //  val coll = bucket.defaultCollection
//  //
//  //  @Test
//  def simple() {
//    //    cluster.searchQuery(SearchQuery("travel-sample-index-unstored",
//    //      SearchQuery.queryString("united")).limit(10)) match {
//    //      case Success(result) =>
//    //        assert(result.errors.isEmpty)
//    //        assert(10 == result.allRowsOrErrors.get.size)
//    //      case Failure(exception) =>
//    //        fail(exception)
//    //    }
//    //  }
//  }
//}
