package com.couchbase.client.performer.scala.util

import com.couchbase.client.core.msg.kv.DurabilityLevel
import com.couchbase.client.core.transaction.support.OptionsUtil
import com.couchbase.client.protocol.shared.{ClusterConfig, ClusterConnectionCreateRequest, Durability}
import com.couchbase.client.scala.codec.{JsonTranscoder, LegacyTranscoder, RawBinaryTranscoder, RawJsonTranscoder, RawStringTranscoder, Transcoder}
import com.couchbase.client.scala.env.{ClusterEnvironment, IoConfig, TimeoutConfig}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

object OptionsUtil {
  def convertClusterConfig(request: ClusterConnectionCreateRequest): Option[ClusterEnvironment.Builder] = {
    var clusterEnvironment: ClusterEnvironment.Builder = null
    if (request.hasClusterConfig) {
      val cc: ClusterConfig = request.getClusterConfig
      clusterEnvironment = ClusterEnvironment.builder
      if (cc.getUseCustomSerializer) {
        // Scala handles serializers differently
        throw new UnsupportedOperationException()
      }
      applyClusterConfig(clusterEnvironment, cc)
      if (cc.hasObservabilityConfig) {
        throw new UnsupportedOperationException()
      }
      if (cc.hasTransactionsConfig) {
        throw new UnsupportedOperationException()
      }
    }
    Option(clusterEnvironment)
  }

  private def applyClusterConfig(clusterEnvironment: ClusterEnvironment.Builder, cc: ClusterConfig): Unit = {
    var ioConfig: IoConfig = null
    var timeoutConfig: TimeoutConfig = null
    if (cc.hasKvConnectTimeoutSecs) {
      if (timeoutConfig == null) {
        timeoutConfig = TimeoutConfig()
      }
      Duration(cc.getKvConnectTimeoutSecs, TimeUnit.SECONDS)
      timeoutConfig = timeoutConfig.connectTimeout(Duration(cc.getKvConnectTimeoutSecs, TimeUnit.SECONDS))
    }
    if (cc.hasKvTimeoutMillis) {
      if (timeoutConfig == null) {
        timeoutConfig = TimeoutConfig()
      }
      timeoutConfig = timeoutConfig.kvTimeout(Duration(cc.getKvTimeoutMillis, TimeUnit.MILLISECONDS))
    }
    if (cc.hasKvDurableTimeoutMillis) {
      if (timeoutConfig == null) {
        timeoutConfig = TimeoutConfig()
      }
      timeoutConfig = timeoutConfig.kvDurableTimeout(Duration(cc.getKvDurableTimeoutMillis, TimeUnit.MILLISECONDS))
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
      timeoutConfig = timeoutConfig.analyticsTimeout(Duration(cc.getAnalyticsTimeoutSecs, TimeUnit.SECONDS))
    }
    if (cc.hasSearchTimeoutSecs) {
      if (timeoutConfig == null) {
        timeoutConfig = TimeoutConfig()
      }
      timeoutConfig = timeoutConfig.searchTimeout(Duration(cc.getSearchTimeoutSecs, TimeUnit.SECONDS))
    }
    if (cc.hasManagementTimeoutSecs) {
      if (timeoutConfig == null) {
        timeoutConfig = TimeoutConfig()
      }
      timeoutConfig = timeoutConfig.managementTimeout(Duration(cc.getManagementTimeoutSecs, TimeUnit.SECONDS))
    }
    if (cc.hasKvScanTimeoutSecs) {
      if (timeoutConfig == null) {
        timeoutConfig = TimeoutConfig()
      }
      timeoutConfig = timeoutConfig.kvScanTimeout(Duration(cc.getKvScanTimeoutSecs, TimeUnit.SECONDS))
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
      ioConfig = ioConfig.tcpKeepAliveTime(Duration(cc.getTcpKeepAliveTimeMillis, TimeUnit.MILLISECONDS))
    }
    if (cc.getForceIPV4) {
      throw new UnsupportedOperationException
    }
    if (cc.hasConfigPollIntervalSecs) {
      if (ioConfig == null) {
        ioConfig = IoConfig()
      }
      ioConfig = ioConfig.configPollInterval(Duration(cc.getConfigPollIntervalSecs, TimeUnit.SECONDS))
    }
    if (cc.hasConfigPollFloorIntervalSecs) {
      throw new UnsupportedOperationException
    }
    if (cc.hasConfigIdleRedialTimeoutSecs) {
      if (ioConfig == null) {
        ioConfig = IoConfig()
      }
      ioConfig = ioConfig.configIdleRedialTimeout(Duration(cc.getConfigIdleRedialTimeoutSecs, TimeUnit.SECONDS))
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
      ioConfig = ioConfig.idleHttpConnectionTimeout(Duration(cc.getIdleHttpConnectionTimeoutSecs, TimeUnit.SECONDS))
    }
    if (ioConfig != null) {
      clusterEnvironment.ioConfig(ioConfig)
    }
    if (timeoutConfig != null) {
      clusterEnvironment.timeoutConfig(timeoutConfig)
    }
  }

  def convertTranscoder(transcoder: com.couchbase.client.protocol.shared.Transcoder): Transcoder = {
    if (transcoder.hasRawJson) RawJsonTranscoder.Instance
    else if (transcoder.hasJson) JsonTranscoder.Instance
    else if (transcoder.hasLegacy) LegacyTranscoder.Instance
    else if (transcoder.hasRawString) RawStringTranscoder.Instance
    else if (transcoder.hasRawBinary) RawBinaryTranscoder.Instance
    else throw new UnsupportedOperationException("Unknown transcoder")
  }

}
