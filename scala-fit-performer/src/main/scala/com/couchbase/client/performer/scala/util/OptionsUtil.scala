package com.couchbase.client.performer.scala.util

import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory
import com.couchbase.client.core.msg.kv.DurabilityLevel
import com.couchbase.client.core.retry.BestEffortRetryStrategy
import com.couchbase.client.protocol.shared.{
  ClusterConfig,
  ClusterConnectionCreateRequest,
  Durability,
  ScanConsistency
}
import com.couchbase.client.scala.codec._
import com.couchbase.client.scala.env.{ClusterEnvironment, IoConfig, SecurityConfig, TimeoutConfig}
import com.couchbase.client.scala.query.{QueryParameters, QueryProfile, QueryScanConsistency}

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.security.cert.{CertificateFactory, X509Certificate}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.CollectionConverters._

// [start:1.5.0]
import com.couchbase.client.core.transaction.cleanup.CleanerMockFactory
import com.couchbase.client.scala.transactions.TransactionKeyspace
import com.couchbase.client.scala.transactions.config.{
  TransactionOptions,
  TransactionsCleanupConfig,
  TransactionsConfig
}
import com.couchbase.client.protocol.transactions.{CommandQuery, TransactionCreateRequest}
// [end:1.5.0]

object OptionsUtil {

  // Have to hardcode these.  The earliest versions of the Scala SDK do not give access to the environment.
  val DefaultManagementTimeout: FiniteDuration      = Duration(75, TimeUnit.SECONDS)
  val DefaultRetryStrategy: BestEffortRetryStrategy = BestEffortRetryStrategy.INSTANCE

  def convertClusterConfig(
      request: ClusterConnectionCreateRequest,
      getCluster: () => ClusterConnection
  ): Option[ClusterEnvironment.Builder] = {
    var clusterEnvironment: ClusterEnvironment.Builder = null
    if (request.hasClusterConfig) {
      val cc: ClusterConfig = request.getClusterConfig
      clusterEnvironment = ClusterEnvironment.builder
      if (cc.getUseCustomSerializer) {
        // Scala handles serializers differently
        throw new UnsupportedOperationException("Cannot handle custom serializer")
      }
      clusterEnvironment = applyClusterConfig(clusterEnvironment, cc)
      if (cc.hasObservabilityConfig) {
        throw new UnsupportedOperationException("Cannot handle observability")
      }
      // [start:1.5.0]
      if (cc.hasTransactionsConfig) {
        clusterEnvironment = applyTransactionsConfig(request, getCluster, clusterEnvironment)
      }
      // [end:1.5.0]
    }
    Option(clusterEnvironment)
  }

  private def applyClusterConfig(
      clusterEnvironment: ClusterEnvironment.Builder,
      cc: ClusterConfig
  ): ClusterEnvironment.Builder = {
    var ioConfig: IoConfig             = null
    var timeoutConfig: TimeoutConfig   = null
    var securityConfig: SecurityConfig = null
    if (cc.hasKvConnectTimeoutSecs) {
      if (timeoutConfig == null) {
        timeoutConfig = TimeoutConfig()
      }
      timeoutConfig =
        timeoutConfig.connectTimeout(Duration(cc.getKvConnectTimeoutSecs, TimeUnit.SECONDS))
    }
    if (cc.hasKvTimeoutMillis) {
      if (timeoutConfig == null) {
        timeoutConfig = TimeoutConfig()
      }
      timeoutConfig =
        timeoutConfig.kvTimeout(Duration(cc.getKvTimeoutMillis, TimeUnit.MILLISECONDS))
    }
    if (cc.hasKvDurableTimeoutMillis) {
      if (timeoutConfig == null) {
        timeoutConfig = TimeoutConfig()
      }
      timeoutConfig = timeoutConfig.kvDurableTimeout(
        Duration(cc.getKvDurableTimeoutMillis, TimeUnit.MILLISECONDS)
      )
    }
    if (cc.hasViewTimeoutSecs) {
      if (timeoutConfig == null) {
        timeoutConfig = TimeoutConfig()
      }
      timeoutConfig = timeoutConfig.viewTimeout(Duration(cc.getViewTimeoutSecs, TimeUnit.SECONDS))
    }
    if (cc.hasQueryTimeoutSecs) {
      if (timeoutConfig == null) {
        timeoutConfig = TimeoutConfig()
      }
      timeoutConfig = timeoutConfig.queryTimeout(Duration(cc.getQueryTimeoutSecs, TimeUnit.SECONDS))
    }
    if (cc.hasAnalyticsTimeoutSecs) {
      if (timeoutConfig == null) {
        timeoutConfig = TimeoutConfig()
      }
      timeoutConfig =
        timeoutConfig.analyticsTimeout(Duration(cc.getAnalyticsTimeoutSecs, TimeUnit.SECONDS))
    }
    if (cc.hasSearchTimeoutSecs) {
      if (timeoutConfig == null) {
        timeoutConfig = TimeoutConfig()
      }
      timeoutConfig =
        timeoutConfig.searchTimeout(Duration(cc.getSearchTimeoutSecs, TimeUnit.SECONDS))
    }
    if (cc.hasManagementTimeoutSecs) {
      if (timeoutConfig == null) {
        timeoutConfig = TimeoutConfig()
      }
      timeoutConfig =
        timeoutConfig.managementTimeout(Duration(cc.getManagementTimeoutSecs, TimeUnit.SECONDS))
    }
    if (cc.hasKvScanTimeoutSecs) {
      if (timeoutConfig == null) {
        timeoutConfig = TimeoutConfig()
      }
      // [start:1.5.0]
      timeoutConfig =
        timeoutConfig.kvScanTimeout(Duration(cc.getKvScanTimeoutSecs, TimeUnit.SECONDS))
      // [end:1.5.0]
    }
    if (cc.hasTranscoder) {
      clusterEnvironment.transcoder(convertTranscoder(cc.getTranscoder))
    }
    if (cc.hasEnableMutationTokens) {
      if (ioConfig == null) {
        ioConfig = IoConfig()
      }
      ioConfig = ioConfig.mutationTokensEnabled(cc.getEnableMutationTokens)
    }
    if (cc.hasTcpKeepAliveTimeMillis) {
      if (ioConfig == null) {
        ioConfig = IoConfig()
      }
      ioConfig =
        ioConfig.tcpKeepAliveTime(Duration(cc.getTcpKeepAliveTimeMillis, TimeUnit.MILLISECONDS))
    }
    if (cc.getForceIPV4) {
      throw new UnsupportedOperationException("Cannot force IPV4")
    }
    if (cc.hasConfigPollIntervalSecs) {
      if (ioConfig == null) {
        ioConfig = IoConfig()
      }
      ioConfig =
        ioConfig.configPollInterval(Duration(cc.getConfigPollIntervalSecs, TimeUnit.SECONDS))
    }
    if (cc.hasConfigPollFloorIntervalSecs) {
      throw new UnsupportedOperationException("Cannot handle hasConfigPollFloorIntervalSecs")
    }
    if (cc.hasConfigIdleRedialTimeoutSecs) {
      if (ioConfig == null) {
        ioConfig = IoConfig()
      }
      ioConfig = ioConfig.configIdleRedialTimeout(
        Duration(cc.getConfigIdleRedialTimeoutSecs, TimeUnit.SECONDS)
      )
    }
    if (cc.hasNumKvConnections) {
      if (ioConfig == null) {
        ioConfig = IoConfig()
      }
      ioConfig = ioConfig.numKvConnections(cc.getNumKvConnections)
    }
    if (cc.hasMaxHttpConnections) {
      if (ioConfig == null) {
        ioConfig = IoConfig()
      }
      ioConfig = ioConfig.maxHttpConnections(cc.getMaxHttpConnections)
    }
    if (cc.hasIdleHttpConnectionTimeoutSecs) {
      if (ioConfig == null) {
        ioConfig = IoConfig()
      }
      ioConfig = ioConfig.idleHttpConnectionTimeout(
        Duration(cc.getIdleHttpConnectionTimeoutSecs, TimeUnit.SECONDS)
      )
    }
    if (cc.getUseTls) {
      if (securityConfig == null) {
        securityConfig = SecurityConfig()
      }
      securityConfig = securityConfig.enableTls(true)
    }
    // [start:1.2.1]
    if (cc.hasCertPath) {
      if (securityConfig == null) {
        securityConfig = SecurityConfig()
      }
      securityConfig = securityConfig.enableTls(true).trustCertificate(Path.of(cc.getCertPath))
    }
    if (cc.hasInsecure) {
      if (securityConfig == null) {
        securityConfig = SecurityConfig()
      }
      // Cannot use enableCertificateVerification as it was added later
      securityConfig = securityConfig.trustManagerFactory(InsecureTrustManagerFactory.INSTANCE)
    }
    // [end:1.2.1]
    if (cc.hasCert) {
      if (securityConfig == null) {
        securityConfig = SecurityConfig()
      }
      val cFactory = CertificateFactory.getInstance("X.509")
      val file     = new ByteArrayInputStream(cc.getCert.getBytes(StandardCharsets.UTF_8))
      val cert     = cFactory.generateCertificate(file).asInstanceOf[X509Certificate]
      securityConfig = securityConfig.enableTls(true).trustCertificates(Seq(cert))
    }
    var out = clusterEnvironment
    if (ioConfig != null) {
      out = clusterEnvironment.ioConfig(ioConfig)
    }
    if (timeoutConfig != null) {
      out = clusterEnvironment.timeoutConfig(timeoutConfig)
    }
    if (securityConfig != null) {
      out = clusterEnvironment.securityConfig(securityConfig)
    }
    out
  }

  def convertTranscoder(transcoder: com.couchbase.client.protocol.shared.Transcoder): Transcoder = {
    if (transcoder.hasRawJson) RawJsonTranscoder.Instance
    else if (transcoder.hasJson) JsonTranscoder.Instance
    else if (transcoder.hasLegacy) LegacyTranscoder.Instance
    else if (transcoder.hasRawString) RawStringTranscoder.Instance
    else if (transcoder.hasRawBinary) RawBinaryTranscoder.Instance
    else throw new UnsupportedOperationException("Unknown transcoder")
  }

  def convertDuration(duration: com.google.protobuf.Duration): FiniteDuration = {
    val nanos = duration.getNanos + TimeUnit.SECONDS.toNanos(duration.getSeconds)
    Duration(nanos, "nanosecond")
  }

  // [start:1.5.0]
  def applyTransactionsConfig(
      request: ClusterConnectionCreateRequest,
      getCluster: () => ClusterConnection,
      clusterEnvironment: ClusterEnvironment.Builder
  ): ClusterEnvironment.Builder = {
    val tc      = request.getClusterConfig.getTransactionsConfig
    var builder = TransactionsConfig()
    val factory = HooksUtil.configureHooks(tc.getHookList.asScala, getCluster)
    val cleanerFactory = new CleanerMockFactory(
      HooksUtil.configureCleanupHooks(tc.getHookList.asScala, getCluster)
    )
    builder = builder.testFactory(factory, cleanerFactory)
    if (tc.hasDurability) {
      val durabilityLevel: DurabilityLevel = tc.getDurability match {
        case Durability.NONE     => DurabilityLevel.NONE
        case Durability.MAJORITY => DurabilityLevel.MAJORITY
        case Durability.MAJORITY_AND_PERSIST_TO_ACTIVE =>
          DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE
        case Durability.PERSIST_TO_MAJORITY => DurabilityLevel.PERSIST_TO_MAJORITY
        case _                              => throw new UnsupportedOperationException()
      }
      builder = builder.durabilityLevel(durabilityLevel)
    }
    if (tc.hasCleanupConfig) {
      val cleanupConfig  = tc.getCleanupConfig
      var cleanupBuilder = TransactionsCleanupConfig()
      if (cleanupConfig.hasCleanupLostAttempts)
        cleanupBuilder = cleanupBuilder.cleanupLostAttempts(cleanupConfig.getCleanupLostAttempts)
      if (cleanupConfig.hasCleanupClientAttempts)
        cleanupBuilder =
          cleanupBuilder.cleanupClientAttempts(cleanupConfig.getCleanupClientAttempts)
      if (cleanupConfig.hasCleanupWindowMillis)
        cleanupBuilder = cleanupBuilder.cleanupWindow(
          Duration(cleanupConfig.getCleanupWindowMillis, TimeUnit.MILLISECONDS)
        )
      if (cleanupConfig.getCleanupCollectionCount > 0)
        cleanupBuilder = cleanupBuilder.collections(
          cleanupConfig.getCleanupCollectionList.asScala
            .map(
              v =>
                TransactionKeyspace(
                  v.getBucketName,
                  Some(v.getScopeName),
                  Some(v.getCollectionName)
                )
            )
            .toSet
        )
      builder = builder.cleanupConfig(cleanupBuilder)
    }
    if (tc.hasTimeoutMillis)
      builder = builder.timeout(Duration(tc.getTimeoutMillis, TimeUnit.MILLISECONDS))
    if (tc.hasMetadataCollection) {
      builder = builder.metadataCollection(
        TransactionKeyspace(
          tc.getMetadataCollection.getBucketName,
          Some(tc.getMetadataCollection.getScopeName),
          Some(tc.getMetadataCollection.getCollectionName)
        )
      )
    }
    clusterEnvironment.transactionsConfig(builder)
  }

  def makeTransactionOptions(
      connection: ClusterConnection,
      req: TransactionCreateRequest,
      spans: collection.Map[String, RequestSpan]
  ): Option[TransactionOptions] = {
    var ptcb: TransactionOptions = null
    if (req.hasOptions) {
      val to = req.getOptions
      ptcb = TransactionOptions()
      if (to.hasDurability) {
        ptcb = ptcb.durabilityLevel(to.getDurability match {
          case Durability.NONE     => DurabilityLevel.NONE
          case Durability.MAJORITY => DurabilityLevel.MAJORITY
          case Durability.MAJORITY_AND_PERSIST_TO_ACTIVE =>
            DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE
          case Durability.PERSIST_TO_MAJORITY => DurabilityLevel.PERSIST_TO_MAJORITY
        })
      }
      if (to.hasMetadataCollection) {
        val mc = to.getMetadataCollection
        ptcb = ptcb.metadataCollection(
          connection.cluster
            .bucket(mc.getBucketName)
            .scope(mc.getScopeName)
            .collection(mc.getCollectionName)
        )
      }
      if (to.hasTimeoutMillis) {
        ptcb = ptcb.timeout(Duration(to.getTimeoutMillis, TimeUnit.MILLISECONDS))
      }
      if (to.getHookCount > 0) {
        val factory = HooksUtil.configureHooks(to.getHookList.asScala, () => connection)
        ptcb = ptcb.testFactory(factory)
      }
      if (to.hasParentSpanId) {
        ptcb = ptcb.parentSpan(spans(to.getParentSpanId))
      }
    }
    Option(ptcb)
  }

  def transactionQueryOptions(
      request: CommandQuery
  ): Option[com.couchbase.client.scala.transactions.TransactionQueryOptions] = {
    var queryOptions: com.couchbase.client.scala.transactions.TransactionQueryOptions = null
    if (request.hasQueryOptions) {
      queryOptions = com.couchbase.client.scala.transactions.TransactionQueryOptions()
      val qo = request.getQueryOptions
      if (qo.hasScanConsistency)
        queryOptions = queryOptions.scanConsistency(qo.getScanConsistency match {
          case ScanConsistency.REQUEST_PLUS =>
            val scanWait: Option[Duration] =
              if (qo.hasScanWaitMillis) Some(Duration(qo.getScanWaitMillis, TimeUnit.MILLISECONDS))
              else None
            QueryScanConsistency.RequestPlus(scanWait)
          case ScanConsistency.NOT_BOUNDED => QueryScanConsistency.NotBounded
        })
      if (qo.getRawCount > 0) queryOptions = queryOptions.raw(qo.getRawMap.asScala.toMap)
      if (qo.hasAdhoc) queryOptions = queryOptions.adhoc(qo.getAdhoc)
      if (qo.hasProfile) queryOptions = queryOptions.profile(qo.getProfile match {
        case "off"     => QueryProfile.Off
        case "phases"  => QueryProfile.Phases
        case "timings" => QueryProfile.Timings
      })
      if (qo.hasReadonly) queryOptions = queryOptions.readonly(qo.getReadonly)
      if (qo.getParametersPositionalCount > 0)
        queryOptions = queryOptions.parameters(
          QueryParameters.Positional(qo.getParametersPositionalList.asScala: _*)
        )
      if (qo.getParametersNamedCount > 0)
        queryOptions =
          queryOptions.parameters(QueryParameters.Named(qo.getParametersNamedMap.asScala))
      if (qo.hasFlexIndex) queryOptions = queryOptions.flexIndex(qo.getFlexIndex)
      if (qo.hasPipelineCap) queryOptions = queryOptions.pipelineCap(qo.getPipelineCap)
      if (qo.hasPipelineBatch) queryOptions = queryOptions.pipelineBatch(qo.getPipelineBatch)
      if (qo.hasScanCap) queryOptions = queryOptions.scanCap(qo.getScanCap)
      if (qo.hasClientContextId) queryOptions = queryOptions.clientContextId(qo.getClientContextId)
    }
    Option(queryOptions)
  }
  // [end:1.5.0]
}
