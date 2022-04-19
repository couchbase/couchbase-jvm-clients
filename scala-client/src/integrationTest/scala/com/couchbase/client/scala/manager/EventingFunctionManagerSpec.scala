package com.couchbase.client.scala.manager

import com.couchbase.client.core.error.{
  BucketNotFoundException,
  CollectionNotFoundException,
  EventingFunctionCompilationFailureException,
  EventingFunctionIdenticalKeyspaceException,
  EventingFunctionNotBootstrappedException,
  EventingFunctionNotDeployedException,
  EventingFunctionNotFoundException
}
import com.couchbase.client.scala.{Cluster, Collection, TestUtils}
import com.couchbase.client.scala.manager.collection.{CollectionManager, CollectionSpec, ScopeSpec}
import com.couchbase.client.scala.manager.eventing.{
  EventingFunction,
  EventingFunctionDcpBoundary,
  EventingFunctionDeploymentStatus,
  EventingFunctionKeyspace,
  EventingFunctionLanguageCompatibility,
  EventingFunctionLogLevel,
  EventingFunctionManager,
  EventingFunctionProcessingStatus,
  EventingFunctionSettings,
  EventingFunctionStatus
}
import com.couchbase.client.scala.query.QueryScanConsistency
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.test.Util.waitUntilCondition
import com.couchbase.client.test.{Capabilities, ClusterAwareIntegrationTest, IgnoreWhen}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.opentest4j.AssertionFailedError

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.util.{Failure, Try}

@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(missesCapabilities = Array(Capabilities.COLLECTIONS, Capabilities.EVENTING))
class EventingFunctionManagerSpec extends ScalaIntegrationTest {
  private var cluster: Cluster                   = _
  private var sourceCollection: Collection       = _
  private var metaCollection: Collection         = _
  private var functions: EventingFunctionManager = _

  @BeforeAll
  def setup(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(ClusterAwareIntegrationTest.config().bucketname())
    cluster.waitUntilReady(Duration(30, TimeUnit.SECONDS))
    functions = cluster.eventingFunctions
    bucket.collections.createScope("eventing")
    bucket.collections.createCollection(CollectionSpec("source", "eventing"))
    bucket.collections.createCollection(CollectionSpec("meta", "eventing"))

    sourceCollection = bucket.scope("eventing").collection("source")
    metaCollection = bucket.scope("eventing").collection("meta")

    waitUntilCondition(
      () =>
        bucket.collections
          .getAllScopes()
          .get
          .exists(
            (s: ScopeSpec) =>
              s.name == "eventing" && s.collections.exists(v => v.name == "source") && s.collections
                .exists(v => v.name == "meta")
          )
    )

    functions.getAllFunctions().get.foreach(f => functions.dropFunction(f.name).get)
  }

  @AfterAll
  def tearDown(): Unit = {
    cluster.disconnect()
  }

  @Test
  def upsertGetAndDropFunction(): Unit = {
    val funcName = UUID.randomUUID.toString

    val function = EventingFunction.create(
      funcName,
      "function OnUpdate(doc, meta) {}",
      EventingFunctionKeyspace(
        sourceCollection.bucketName,
        Some(sourceCollection.scopeName),
        Some(sourceCollection.name)
      ),
      EventingFunctionKeyspace(
        metaCollection.bucketName,
        Some(metaCollection.scopeName),
        Some(metaCollection.name)
      )
    )

    functions.upsertFunction(function).get

    val read = functions.getFunction(funcName).get

    assert(read.metadataKeyspace.collection.contains(metaCollection.name))
    assert(read.sourceKeyspace.collection.contains(sourceCollection.name))
    assert("function OnUpdate(doc, meta) {}" == read.code)
    assert(functions.getAllFunctions().get.exists(f => f.name == funcName))

    functions.dropFunction(funcName)
    assert(!functions.getAllFunctions().get.exists(f => f.name == funcName))
  }

  @Test
  def setAndGetAlLSettings(): Unit = {
    val funcName = UUID.randomUUID.toString

    val settings = EventingFunctionSettings(
      Some(5),
      Some(EventingFunctionDcpBoundary.FromNow),
      Some("desc"),
      Some(EventingFunctionLogLevel.Debug),
      Some(EventingFunctionLanguageCompatibility.Version_6_0_0),
      Some(Duration.create(5, TimeUnit.SECONDS)),
      Some(6),
      Some(7),
      Some(Duration.create(8, TimeUnit.SECONDS)),
      Some(QueryScanConsistency.RequestPlus()),
      Some(9),
      Some(10),
      Some(Duration.create(30, TimeUnit.SECONDS)),
      Some(31),
      Some("prefix"),
      Some(13),
      Some(14),
      Some(15),
      Some(16),
      Some(true),
      None,
      None,
      Some(true),
      None,
      Some(17),
      Some(18),
      Some(Duration.create(19, TimeUnit.SECONDS))
    )
    val function = EventingFunction(
      funcName,
      "function OnUpdate(doc, meta) {}",
      EventingFunctionKeyspace(
        sourceCollection.bucketName,
        Some(sourceCollection.scopeName),
        Some(sourceCollection.name)
      ),
      EventingFunctionKeyspace(
        metaCollection.bucketName,
        Some(metaCollection.scopeName),
        Some(metaCollection.name)
      ),
      Some(settings)
    )

    functions.upsertFunction(function).get

    val read = functions.getFunction(funcName).get

    assert(read.metadataKeyspace.collection.contains(metaCollection.name))
    assert(read.sourceKeyspace.collection.contains(sourceCollection.name))
    assert("function OnUpdate(doc, meta) {}" == read.code)
    assert(read.settings.get.cppWorkerThreadCount == settings.cppWorkerThreadCount)
    assert(read.settings.get.dcpStreamBoundary == settings.dcpStreamBoundary)
    assert(read.settings.get.description == settings.description)
    assert(read.settings.get.logLevel == settings.logLevel)
    assert(read.settings.get.languageCompatibility == settings.languageCompatibility)
    assert(read.settings.get.executionTimeout == settings.executionTimeout)
    assert(read.settings.get.lcbInstCapacity == settings.lcbInstCapacity)
    assert(read.settings.get.lcbRetryCount == settings.lcbRetryCount)
    assert(read.settings.get.lcbTimeout == settings.lcbTimeout)
    assert(read.settings.get.queryConsistency == settings.queryConsistency)
    assert(read.settings.get.numTimerPartitions == settings.numTimerPartitions)
    assert(read.settings.get.sockBatchSize == settings.sockBatchSize)
    assert(read.settings.get.tickDuration == settings.tickDuration)
    assert(read.settings.get.timerContextSize == settings.timerContextSize)
    assert(read.settings.get.userPrefix == settings.userPrefix)
    assert(read.settings.get.bucketCacheSize == settings.bucketCacheSize)
    assert(read.settings.get.bucketCacheAge == settings.bucketCacheAge)
    assert(read.settings.get.curlMaxAllowedRespSize == settings.curlMaxAllowedRespSize)
    assert(read.settings.get.workerCount == settings.workerCount)
    assert(read.settings.get.queryPrepareAll == settings.queryPrepareAll)
    assert(read.settings.get.handlerHeaders == settings.handlerHeaders)
    assert(read.settings.get.handlerFooters == settings.handlerFooters)
    assert(read.settings.get.enableAppLogRotation == settings.enableAppLogRotation)
    assert(read.settings.get.appLogDir == settings.appLogDir)
    assert(read.settings.get.appLogMaxSize == settings.appLogMaxSize)
    assert(read.settings.get.appLogMaxFiles == settings.appLogMaxFiles)
    assert(functions.getAllFunctions().get.exists(f => f.name == funcName))

    functions.dropFunction(funcName)
    assert(!functions.getAllFunctions().get.exists(f => f.name == funcName))
  }

  @Test
  def failsWithUnknownFunctionName(): Unit = {
    val funcName = UUID.randomUUID.toString

    functions.getFunction(funcName) match {
      case Failure(err: EventingFunctionNotFoundException) =>
      case x                                               => assert(false, s"Unexpected result ${x}")
    }
    functions.deployFunction(funcName) match {
      case Failure(err: EventingFunctionNotFoundException) =>
      case x                                               => assert(false, s"Unexpected result ${x}")
    }
    functions.pauseFunction(funcName) match {
      case Failure(err: EventingFunctionNotFoundException) =>
      case x                                               => assert(false, s"Unexpected result ${x}")
    }

    // See MB-47840 on why those are not EventingFunctionNotFoundException
    // Update: fixed in 7.1, so allow either result
    functions.dropFunction(funcName) match {
      case Failure(err: EventingFunctionNotDeployedException) =>
      case Failure(err: EventingFunctionNotFoundException)    =>
      case x                                                  => assert(false, s"Unexpected result ${x}")
    }
    functions.undeployFunction(funcName) match {
      case Failure(err: EventingFunctionNotDeployedException) =>
      case Failure(err: EventingFunctionNotFoundException)    =>
      case x                                                  => assert(false, s"Unexpected result ${x}")
    }
    functions.resumeFunction(funcName) match {
      case Failure(err: EventingFunctionNotDeployedException) =>
      case Failure(err: EventingFunctionNotFoundException)    =>
      case x                                                  => assert(false, s"Unexpected result ${x}")
    }
  }

  @Test
  def failsIfCodeIsInvalid(): Unit = {
    val funcName = UUID.randomUUID.toString
    val function = EventingFunction.create(
      funcName,
      "someInvalidFunc",
      EventingFunctionKeyspace.createFrom(sourceCollection),
      EventingFunctionKeyspace.createFrom(metaCollection)
    )
    functions.upsertFunction(function) match {
      case Failure(err: EventingFunctionCompilationFailureException) =>
      case x                                                         => assert(false, s"Unexpected result ${x}")
    }
  }

  @Test
  def failsIfCollectionNotFound(): Unit = {
    val funcName = UUID.randomUUID.toString
    val function = EventingFunction.create(
      funcName,
      "function OnUpdate(doc, meta) {}",
      EventingFunctionKeyspace.createFrom(sourceCollection),
      EventingFunctionKeyspace(
        metaCollection.bucketName,
        Some(metaCollection.scopeName),
        Some("noIdeaWhatThisIs")
      )
    )
    functions.upsertFunction(function) match {
      case Failure(err: CollectionNotFoundException) =>
      case x                                         => assert(false, s"Unexpected result ${x}")
    }
  }

  @Test
  def failsIfSourceAndMetaSame(): Unit = {
    val funcName = UUID.randomUUID.toString
    val function = EventingFunction.create(
      funcName,
      "function OnUpdate(doc, meta) {}",
      EventingFunctionKeyspace.createFrom(sourceCollection),
      EventingFunctionKeyspace.createFrom(sourceCollection)
    )
    functions.upsertFunction(function) match {
      case Failure(err: EventingFunctionIdenticalKeyspaceException) =>
      case x                                                        => assert(false, s"Unexpected result ${x}")
    }
  }

  @Test
  def failsIfBucketDoesNotExist(): Unit = {
    val funcName = UUID.randomUUID.toString
    val function = EventingFunction.create(
      funcName,
      "function OnUpdate(doc, meta) {}",
      EventingFunctionKeyspace.createFrom(sourceCollection),
      EventingFunctionKeyspace("bar", Some(metaCollection.scopeName), Some(metaCollection.name))
    )
    functions.upsertFunction(function) match {
      case Failure(err: BucketNotFoundException) =>
      case x                                     => assert(false, s"Unexpected result ${x}")
    }
  }

  @Test
  def deploysAndUndeploysFunction(): Unit = {
    val funcName = UUID.randomUUID.toString
    val function = EventingFunction.create(
      funcName,
      "function OnUpdate(doc, meta) {}",
      EventingFunctionKeyspace.createFrom(sourceCollection),
      EventingFunctionKeyspace.createFrom(metaCollection)
    )
    functions.upsertFunction(function).get
    var read = functions.getFunction(funcName).get
    read.settings.get.deploymentStatus match {
      case Some(EventingFunctionDeploymentStatus.Undeployed) =>
      case _                                                 => assert(false, s"Unexpected result ${read.settings.get.deploymentStatus}")
    }
    functions.undeployFunction(funcName) match {
      case Failure(err: EventingFunctionNotDeployedException) =>
      case x                                                  => assert(false, s"Unexpected result ${x}")
    }
    functions.deployFunction(funcName).get
    waitUntilCondition(() => isState(funcName, EventingFunctionStatus.Deployed))
    read = functions.getFunction(funcName).get
    read.settings.get.deploymentStatus match {
      case Some(EventingFunctionDeploymentStatus.Deployed) =>
      case _                                               => assert(false, s"Unexpected result ${read.settings.get.deploymentStatus}")
    }
    functions.undeployFunction(funcName).get
    waitUntilCondition(() => isState(funcName, EventingFunctionStatus.Undeployed))
    read = functions.getFunction(funcName).get
    read.settings.get.deploymentStatus match {
      case Some(EventingFunctionDeploymentStatus.Undeployed) =>
      case _                                                 => assert(false, s"Unexpected result ${read.settings.get.deploymentStatus}")
    }
    functions.dropFunction(funcName).get
  }

  @Test
  def pausesAndResumesFunction(): Unit = {
    val funcName = UUID.randomUUID.toString
    val function = EventingFunction.create(
      funcName,
      "function OnUpdate(doc, meta) {}",
      EventingFunctionKeyspace.createFrom(sourceCollection),
      EventingFunctionKeyspace.createFrom(metaCollection)
    )
    functions.upsertFunction(function).get
    var read = functions.getFunction(funcName).get
    read.settings.get.processingStatus match {
      case Some(EventingFunctionProcessingStatus.Paused) =>
      case _                                             => assert(false, s"Unexpected result ${read.settings.get.processingStatus}")
    }
    functions.pauseFunction(funcName) match {
      case Failure(err: EventingFunctionNotBootstrappedException) =>
      case x                                                      => assert(false, s"Unexpected result ${x}")
    }
    functions.resumeFunction(funcName) match {
      case Failure(err: EventingFunctionNotDeployedException) =>
      case x                                                  => assert(false, s"Unexpected result ${x}")
    }
    functions.deployFunction(funcName).get
    waitUntilCondition(() => isState(funcName, EventingFunctionStatus.Deployed))
    read = functions.getFunction(funcName).get
    read.settings.get.processingStatus match {
      case Some(EventingFunctionProcessingStatus.Running) =>
      case _                                              => assert(false, s"Unexpected result ${read.settings.get.processingStatus}")
    }
    functions.pauseFunction(funcName).get
    waitUntilCondition(() => isState(funcName, EventingFunctionStatus.Paused))
    read = functions.getFunction(funcName).get
    read.settings.get.processingStatus match {
      case Some(EventingFunctionProcessingStatus.Paused) =>
      case _                                             => assert(false, s"Unexpected result ${read.settings.get.processingStatus}")
    }
    functions.undeployFunction(funcName).get
    waitUntilCondition(() => isState(funcName, EventingFunctionStatus.Undeployed))
    functions.dropFunction(funcName).get
  }

  private def isState(funcName: String, status: EventingFunctionStatus): Boolean = {
    functions
      .functionsStatus()
      .get
      .functions
      .exists(state => state.name == funcName && (state.status == status))
  }

}
