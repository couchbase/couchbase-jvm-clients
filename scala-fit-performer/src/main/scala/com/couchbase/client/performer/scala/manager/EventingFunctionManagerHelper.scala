package com.couchbase.client.performer.scala.manager

// [skip:<1.6.2]

import com.couchbase.client.performer.scala.ScalaSdkCommandExecutor.convertDuration
import com.couchbase.client.performer.scala.util.OptionsUtil
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.scala.Cluster
import com.couchbase.client.scala.manager.eventing._
import com.couchbase.client.scala.query.QueryScanConsistency
import com.couchbase.client.scala.util.DurationConversions._

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

object EventingFunctionManagerHelper {

  def handleClusterEventingFunctionManager(
      cluster: Cluster,
      command: com.couchbase.client.protocol.sdk.Command
  ): com.couchbase.client.protocol.run.Result = {
    val sim                                = command.getClusterCommand.getEventingFunctionManager
    val em                                 = cluster.eventingFunctions
    val defaultManagementTimeout: Duration = em.DefaultTimeout

    sim.getCommandCase match {
      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.Command.CommandCase.GET_FUNCTION =>
        val req = sim.getGetFunction

        if (req.hasOptions) {
          val opts              = req.getOptions
          val timeout: Duration =
            if (opts.hasTimeout) OptionsUtil.convertDuration(opts.getTimeout)
            else defaultManagementTimeout
          convertGetFunctionResultToGRPC(em.getFunction(req.getName, timeout).get)
        } else {
          convertGetFunctionResultToGRPC(em.getFunction(req.getName).get)
        }

      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.Command.CommandCase.GET_ALL_FUNCTIONS =>
        val req = sim.getGetAllFunctions

        if (req.hasOptions) {
          val opts              = req.getOptions
          val timeout: Duration =
            if (opts.hasTimeout) OptionsUtil.convertDuration(opts.getTimeout)
            else defaultManagementTimeout
          convertGetAllFunctionsResult(em.getAllFunctions(timeout).get)
        } else {
          convertGetAllFunctionsResult(em.getAllFunctions().get)
        }

      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.Command.CommandCase.DEPLOY_FUNCTION =>
        val req = sim.getDeployFunction

        if (req.hasOptions) {
          val opts              = req.getOptions
          val timeout: Duration =
            if (opts.hasTimeout) OptionsUtil.convertDuration(opts.getTimeout)
            else defaultManagementTimeout
          em.deployFunction(req.getName, timeout).get
          returnSuccess()
        } else {
          em.deployFunction(req.getName).get
          returnSuccess()
        }

      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.Command.CommandCase.UNDEPLOY_FUNCTION =>
        val req = sim.getUndeployFunction

        if (req.hasOptions) {
          val opts              = req.getOptions
          val timeout: Duration =
            if (opts.hasTimeout) OptionsUtil.convertDuration(opts.getTimeout)
            else defaultManagementTimeout
          em.undeployFunction(req.getName, timeout).get
          returnSuccess()
        } else {
          em.undeployFunction(req.getName).get
          returnSuccess()
        }

      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.Command.CommandCase.DROP_FUNCTION =>
        val req = sim.getDropFunction

        if (req.hasOptions) {
          val opts              = req.getOptions
          val timeout: Duration =
            if (opts.hasTimeout) OptionsUtil.convertDuration(opts.getTimeout)
            else defaultManagementTimeout
          em.dropFunction(req.getName, timeout).get
          returnSuccess()
        } else {
          em.dropFunction(req.getName).get
          returnSuccess()
        }

      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.Command.CommandCase.PAUSE_FUNCTION =>
        val req = sim.getPauseFunction

        if (req.hasOptions) {
          val opts              = req.getOptions
          val timeout: Duration =
            if (opts.hasTimeout) OptionsUtil.convertDuration(opts.getTimeout)
            else defaultManagementTimeout
          em.pauseFunction(req.getName, timeout).get
          returnSuccess()
        } else {
          em.pauseFunction(req.getName).get
          returnSuccess()
        }

      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.Command.CommandCase.RESUME_FUNCTION =>
        val req = sim.getResumeFunction

        if (req.hasOptions) {
          val opts              = req.getOptions
          val timeout: Duration =
            if (opts.hasTimeout) OptionsUtil.convertDuration(opts.getTimeout)
            else defaultManagementTimeout
          em.resumeFunction(req.getName, timeout).get
          returnSuccess()
        } else {
          em.resumeFunction(req.getName).get
          returnSuccess()
        }

      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.Command.CommandCase.FUNCTIONS_STATUS =>
        val req = sim.getFunctionsStatus

        if (req.hasOptions) {
          val opts              = req.getOptions
          val timeout: Duration =
            if (opts.hasTimeout) OptionsUtil.convertDuration(opts.getTimeout)
            else defaultManagementTimeout
          convertFunctionsStatusResultToGRPC(em.functionsStatus(timeout).get)
        } else {
          convertFunctionsStatusResultToGRPC(em.functionsStatus().get)
        }

      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.Command.CommandCase.UPSERT_FUNCTION =>
        val req = sim.getUpsertFunction
        val ef  = convertToSdk(req.getFunction)

        if (req.hasOptions) {
          val opts              = req.getOptions
          val timeout: Duration =
            if (opts.hasTimeout) OptionsUtil.convertDuration(opts.getTimeout)
            else defaultManagementTimeout
          em.upsertFunction(ef, timeout).get
          returnSuccess()
        } else {
          em.upsertFunction(ef).get
          returnSuccess()
        }

    }
  }

  private def returnSuccess(): com.couchbase.client.protocol.run.Result = {
    com.couchbase.client.protocol.run.Result.newBuilder
      .setSdk(
        com.couchbase.client.protocol.sdk.Result.newBuilder
          .setSuccess(true)
      )
      .build
  }

  private def convertGetAllFunctionsResult(in: Seq[EventingFunction]): Result = {
    com.couchbase.client.protocol.run.Result.newBuilder
      .setSdk(
        com.couchbase.client.protocol.sdk.Result.newBuilder
          .setEventingFunctionManagerResult(
            com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.Result.newBuilder
              .setEventingFunctionList(
                com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionList.newBuilder
                  .addAllFunctions(in.map(convertToGRPC).asJava)
              )
          )
      )
      .build
  }

  private def convertGetFunctionResultToGRPC(in: EventingFunction): Result = {
    com.couchbase.client.protocol.run.Result.newBuilder
      .setSdk(
        com.couchbase.client.protocol.sdk.Result.newBuilder
          .setEventingFunctionManagerResult(
            com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.Result.newBuilder
              .setEventingFunction(convertToGRPC(in))
          )
      )
      .build
  }

  private def convertFunctionsStatusResultToGRPC(in: EventingStatus): Result = {
    com.couchbase.client.protocol.run.Result.newBuilder
      .setSdk(
        com.couchbase.client.protocol.sdk.Result.newBuilder
          .setEventingFunctionManagerResult(
            com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.Result.newBuilder
              .setEventingStatus(convertToGRPC(in))
          )
      )
      .build

  }

  private def convertToGRPC(
      in: EventingStatus
  ): com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingStatus = {
    com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingStatus.newBuilder
      .addAllFunctions(in.functions.map(v => convertToGRPC(v)).asJava)
      .setNumEventingNodes(in.numEventingNodes)
      .build
  }

  private def convertToGRPC(
      in: EventingFunctionState
  ): com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionState = {
    com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionState.newBuilder
      .setDeploymentStatus(convertToGRPC(in.deploymentStatus))
      .setName(in.name)
      .setNumBootstrappingNodes(in.numBootstrappingNodes)
      .setNumDeployedNodes(in.numDeployedNodes)
      .setProcessingStatus(convertToGRPC(in.processingStatus))
      .setStatus(convertToGRPC(in.status))
      .build
  }

  private def convertToGRPC(
      in: EventingFunction
  ): com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunction = {
    val out =
      com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunction.newBuilder
        .setCode(in.code)
        .setMetadataKeyspace(convertToGRPC(in.metadataKeyspace))
        .setName(in.name)
        .setSourceKeyspace(convertToGRPC(in.sourceKeyspace))

    // Bucket bindings are optional in Scala but should not be
    in.bucketBindings.foreach(bucketBindings => {
      val mapped = bucketBindings.map(v =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionBucketBinding.newBuilder
          .setAccess(v.access match {
            case EventingFunctionBucketAccess.ReadOnly =>
              com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionBucketAccess.READ_ONLY
            case EventingFunctionBucketAccess.ReadWrite =>
              com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionBucketAccess.READ_WRITE
          })
          .setAlias(v.alias)
          .setName(convertToGRPC(v.name))
          .build
      )
      out.addAllBucketBindings(mapped.asJava)
    })

    // Constant bindings are optional in Scala but should not be
    in.constantBindings.foreach(constantBindings => {
      val mapped = constantBindings.map(v =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionConstantBinding.newBuilder
          .setAlias(v.alias)
          .setLiteral(v.literal)
          .build
      )
      out.addAllConstantBindings(mapped.asJava)
    })

    in.enforceSchema.foreach(v => out.setEnforceSchema(v))
    in.functionInstanceId.foreach(v => out.setFunctionInstanceId(v))
    in.handlerUuid.foreach(v => out.setHandlerUuid(v))

    // Settings are optional in Scala but should not be
    in.settings.foreach(v => out.setSettings(convertToGRPC(v)))

    in.version.foreach(v => out.setVersion(v))

    // Url bindings are optional in Scala but should not be
    in.urlBindings.foreach(urlBindings => {
      val mapped = urlBindings.map(v =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionUrlBinding.newBuilder
          .setAlias(v.alias)
          .setAllowCookies(v.allowCookies)
          .setAuth(convertToGRPC(v.auth))
          .setHostname(v.hostname)
          .setValidateSslCertificate(v.validateSslCertificate)
          .build
      )
      out.addAllUrlBindings(mapped.asJava)
    })

    out.build
  }

  private def convertToGRPC(
      in: EventingFunctionKeyspace
  ): com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionKeyspace = {
    val out =
      com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionKeyspace.newBuilder
        .setBucket(in.bucket)
    in.scope.foreach(scope => out.setScope(scope))
    in.collection.foreach(coll => out.setCollection(coll))
    out.build
  }

  private def convertToGRPC(
      in: EventingFunctionUrlAuth
  ): com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionUrlAuth = {
    val out =
      com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionUrlAuth.newBuilder
    in match {
      case EventingFunctionUrlAuth.None =>
        out
          .setNoAuth(
            com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionUrlNoAuth.getDefaultInstance
          )
          .build
      case EventingFunctionUrlAuth.Basic(username, password) =>
        val ret =
          com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionUrlAuthBasic.newBuilder
            .setUsername(username)
        // Should not be optional in Scala
        password.foreach(v => ret.setPassword(v))
        out.setBasicAuth(ret).build
      case EventingFunctionUrlAuth.Digest(username, password) =>
        val ret =
          com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionUrlAuthDigest.newBuilder
            .setUsername(username)
        // Should not be optional in Scala
        password.foreach(v => ret.setPassword(v))
        out.setDigestAuth(ret).build
      case EventingFunctionUrlAuth.Bearer(key) =>
        out
          .setBearerAuth(
            com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionUrlAuthBearer.newBuilder
              .setKey(key)
          )
          .build
    }
  }

  private def convertToGRPC(
      in: EventingFunctionSettings
  ): com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionSettings = {
    val out =
      com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionSettings.newBuilder

    in.appLogDir.foreach(v => out.setAppLogDir(v))
    in.appLogMaxFiles.foreach(v => out.setAppLogMaxFiles(v))
    in.appLogMaxSize.foreach(v => out.setAppLogMaxSize(v))
    in.bucketCacheAge.foreach(v => out.setBucketCacheAge(v))
    in.bucketCacheSize.foreach(v => out.setBucketCacheSize(v))
    in.checkpointInterval.foreach(v => out.setCheckpointInterval(convertDuration(v)))
    in.cppWorkerThreadCount.foreach(v => out.setCppWorkerThreadCount(v))
    in.curlMaxAllowedRespSize.foreach(v => out.setCurlMaxAllowedRespSize(v))
    in.dcpStreamBoundary.foreach(v => out.setDcpStreamBoundary(convertToGRPC(v)))
    in.deploymentStatus.foreach(v => out.setDeploymentStatus(convertToGRPC(v)))
    in.description.foreach(v => out.setDescription(v))
    in.enableAppLogRotation.foreach(v => out.setEnableAppLogRotation(v))
    in.executionTimeout.foreach(v => out.setExecutionTimeout(convertDuration(v)))
    in.handlerFooters.foreach(v => out.addAllHandlerFooters(v.asJava))
    in.handlerHeaders.foreach(v => out.addAllHandlerHeaders(v.asJava))
    in.languageCompatibility.foreach(v => out.setLanguageCompatibility(convertToGRPC(v)))
    in.lcbInstCapacity.foreach(v => out.setLcbInstCapacity(v))
    in.lcbRetryCount.foreach(v => out.setLcbRetryCount(v))
    in.lcbTimeout.foreach(v => out.setLcbTimeout(convertDuration(v)))
    in.logLevel.foreach(v => out.setLogLevel(convertToGRPC(v)))
    in.numTimerPartitions.foreach(v => out.setNumTimerPartitions(v))
    in.processingStatus.foreach(v => out.setProcessingStatus(convertToGRPC(v)))
    in.queryConsistency.foreach(v => out.setQueryConsistency(convertToGRPC(v)))
    in.queryPrepareAll.foreach(v => out.setQueryPrepareAll(v))
    in.sockBatchSize.foreach(v => out.setSockBatchSize(v))
    in.tickDuration.foreach(v => out.setTickDuration(convertDuration(v)))
    in.timerContextSize.foreach(v => out.setTimerContextSize(v))
    in.userPrefix.foreach(v => out.setUserPrefix(v))
    in.workerCount.foreach(v => out.setWorkerCount(v))

    out.build
  }

  private def convertToGRPC(
      in: EventingFunctionDcpBoundary
  ): com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionDcpBoundary = {
    in match {
      case EventingFunctionDcpBoundary.Everything =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionDcpBoundary.EVERYTHING
      case EventingFunctionDcpBoundary.FromNow =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionDcpBoundary.FROM_NOW
    }
  }

  private def convertToGRPC(
      in: EventingFunctionDeploymentStatus
  ): com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionDeploymentStatus = {
    in match {
      case EventingFunctionDeploymentStatus.Deployed =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionDeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED
      case EventingFunctionDeploymentStatus.Undeployed =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionDeploymentStatus.DEPLOYMENT_STATUS_UNDEPLOYED
    }
  }

  private def convertToGRPC(
      in: EventingFunctionLanguageCompatibility
  ): com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLanguageCompatibility = {
    in match {
      case EventingFunctionLanguageCompatibility.Version_6_0_0 =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLanguageCompatibility.VERSION_6_0_0
      case EventingFunctionLanguageCompatibility.Version_6_5_0 =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLanguageCompatibility.VERSION_6_5_0
      case EventingFunctionLanguageCompatibility.Version_6_6_2 =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLanguageCompatibility.VERSION_6_6_2
      case EventingFunctionLanguageCompatibility.Version_7_2_0 =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLanguageCompatibility.VERSION_7_2_0
    }
  }

  private def convertToGRPC(
      in: EventingFunctionStatus
  ): com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionStatus = {
    in match {
      case EventingFunctionStatus.Undeployed =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionStatus.UNDEPLOYED
      case EventingFunctionStatus.Deploying =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionStatus.DEPLOYING
      case EventingFunctionStatus.Deployed =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionStatus.DEPLOYED
      case EventingFunctionStatus.Undeploying =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionStatus.UNDEPLOYING
      case EventingFunctionStatus.Paused =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionStatus.PAUSED
      case EventingFunctionStatus.Pausing =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionStatus.PAUSING
    }
  }

  private def convertToGRPC(
      in: QueryScanConsistency
  ): com.couchbase.client.protocol.shared.ScanConsistency = {
    in match {
      case QueryScanConsistency.NotBounded =>
        com.couchbase.client.protocol.shared.ScanConsistency.NOT_BOUNDED
      case _: QueryScanConsistency.RequestPlus =>
        com.couchbase.client.protocol.shared.ScanConsistency.REQUEST_PLUS
    }
  }

  private def convertToGRPC(
      in: EventingFunctionLogLevel
  ): com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLogLevel = {
    in match {
      case EventingFunctionLogLevel.Info =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLogLevel.INFO
      case EventingFunctionLogLevel.Error =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLogLevel.ERROR
      case EventingFunctionLogLevel.Warning =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLogLevel.WARNING
      case EventingFunctionLogLevel.Debug =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLogLevel.DEBUG
      case EventingFunctionLogLevel.Trace =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLogLevel.TRACE
    }
  }

  private def convertToGRPC(
      in: EventingFunctionProcessingStatus
  ): com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionProcessingStatus = {
    in match {
      case EventingFunctionProcessingStatus.Running =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionProcessingStatus.PROCESSING_STATUS_RUNNING
      case EventingFunctionProcessingStatus.Paused =>
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionProcessingStatus.PROCESSING_STATUS_PAUSED
    }
  }

  private def convertToSdk(
      in: com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunction
  ): EventingFunction = {
    EventingFunction(
      in.getName,
      in.getCode,
      convertToSdk(in.getSourceKeyspace),
      convertToSdk(in.getMetadataKeyspace),
      if (in.hasSettings) Some(convertToSdk(in.getSettings)) else None,
      if (in.hasVersion) Some(in.getVersion) else None,
      if (in.hasEnforceSchema) Some(in.getEnforceSchema) else None,
      if (in.hasHandlerUuid) Some(in.getHandlerUuid) else None,
      if (in.hasFunctionInstanceId) Some(in.getFunctionInstanceId) else None,
      if (in.getBucketBindingsCount > 0)
        Some(convertBucketBindingsToSdk(in.getBucketBindingsList.asScala))
      else None,
      if (in.getUrlBindingsCount > 0) Some(convertUrlBindingsToSdk(in.getUrlBindingsList.asScala))
      else None,
      if (in.getConstantBindingsCount > 0)
        Some(convertConstantBindingsToSdk(in.getConstantBindingsList.asScala))
      else None
    )
  }

  private def convertToSdk(
      in: com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionKeyspace
  ): EventingFunctionKeyspace = {
    EventingFunctionKeyspace(
      in.getBucket,
      if (in.hasScope) Some(in.getScope) else None,
      if (in.hasCollection) Some(in.getCollection) else None
    )
  }

  private def convertToSdk(
      in: com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionSettings
  ): EventingFunctionSettings = {
    EventingFunctionSettings(
      if (in.hasCppWorkerThreadCount) Some(in.getCppWorkerThreadCount) else None,
      if (in.hasDcpStreamBoundary) Some(convertToSdk(in.getDcpStreamBoundary)) else None,
      if (in.hasDescription) Some(in.getDescription) else None,
      if (in.hasLogLevel) Some(convertToSdk(in.getLogLevel)) else None,
      if (in.hasLanguageCompatibility) Some(convertToSdk(in.getLanguageCompatibility)) else None,
      if (in.hasExecutionTimeout) Some(OptionsUtil.convertDuration(in.getExecutionTimeout))
      else None,
      if (in.hasLcbInstCapacity) Some(in.getLcbInstCapacity) else None,
      if (in.hasLcbRetryCount) Some(in.getLcbRetryCount) else None,
      if (in.hasLcbTimeout) Some(OptionsUtil.convertDuration(in.getLcbTimeout)) else None,
      if (in.hasQueryConsistency) Some(convertToSdk(in.getQueryConsistency)) else None,
      if (in.hasNumTimerPartitions) Some(in.getNumTimerPartitions) else None,
      if (in.hasSockBatchSize) Some(in.getSockBatchSize) else None,
      if (in.hasTickDuration) Some(OptionsUtil.convertDuration(in.getTickDuration)) else None,
      if (in.hasTimerContextSize) Some(in.getTimerContextSize) else None,
      if (in.hasUserPrefix) Some(in.getUserPrefix) else None,
      if (in.hasBucketCacheSize) Some(in.getBucketCacheSize) else None,
      if (in.hasBucketCacheAge) Some(in.getBucketCacheAge) else None,
      if (in.hasCurlMaxAllowedRespSize) Some(in.getCurlMaxAllowedRespSize) else None,
      if (in.hasWorkerCount) Some(in.getWorkerCount) else None,
      if (in.hasQueryPrepareAll) Some(in.getQueryPrepareAll) else None,
      if (in.getHandlerHeadersCount > 0) Some(in.getHandlerHeadersList.asScala) else None,
      if (in.getHandlerFootersCount > 0) Some(in.getHandlerFootersList.asScala) else None,
      if (in.hasEnableAppLogRotation) Some(in.getEnableAppLogRotation) else None,
      if (in.hasAppLogDir) Some(in.getAppLogDir) else None,
      if (in.hasAppLogMaxSize) Some(in.getAppLogMaxSize) else None,
      if (in.hasAppLogMaxFiles) Some(in.getAppLogMaxFiles) else None,
      if (in.hasCheckpointInterval) Some(OptionsUtil.convertDuration(in.getCheckpointInterval))
      else None,
      if (in.hasProcessingStatus) Some(convertToSdk(in.getProcessingStatus)) else None,
      if (in.hasDeploymentStatus) Some(convertToSdk(in.getDeploymentStatus)) else None
    )
  }

  private def convertToSdk(
      in: com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLanguageCompatibility
  ): EventingFunctionLanguageCompatibility = {
    in match {
      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLanguageCompatibility.VERSION_6_0_0 =>
        EventingFunctionLanguageCompatibility.Version_6_0_0
      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLanguageCompatibility.VERSION_6_5_0 =>
        EventingFunctionLanguageCompatibility.Version_6_5_0
      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLanguageCompatibility.VERSION_6_6_2 =>
        EventingFunctionLanguageCompatibility.Version_6_6_2
      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLanguageCompatibility.VERSION_7_2_0 =>
        EventingFunctionLanguageCompatibility.Version_7_2_0
    }
  }

  private def convertToSdk(
      in: com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionDcpBoundary
  ): EventingFunctionDcpBoundary = {
    in match {
      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionDcpBoundary.EVERYTHING =>
        EventingFunctionDcpBoundary.Everything
      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionDcpBoundary.FROM_NOW =>
        EventingFunctionDcpBoundary.FromNow
    }
  }

  private def convertToSdk(
      in: com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLogLevel
  ): EventingFunctionLogLevel = {
    in match {
      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLogLevel.DEBUG =>
        EventingFunctionLogLevel.Debug
      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLogLevel.ERROR =>
        EventingFunctionLogLevel.Error
      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLogLevel.INFO =>
        EventingFunctionLogLevel.Info
      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLogLevel.TRACE =>
        EventingFunctionLogLevel.Trace
      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLogLevel.WARNING =>
        EventingFunctionLogLevel.Warning
    }
  }

  private def convertToSdk(
      in: com.couchbase.client.protocol.shared.ScanConsistency
  ): QueryScanConsistency = {
    in match {
      case com.couchbase.client.protocol.shared.ScanConsistency.REQUEST_PLUS =>
        QueryScanConsistency.RequestPlus()
      case com.couchbase.client.protocol.shared.ScanConsistency.NOT_BOUNDED =>
        QueryScanConsistency.NotBounded
    }
  }

  private def convertToSdk(
      in: com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionProcessingStatus
  ): EventingFunctionProcessingStatus = {
    in match {
      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionProcessingStatus.PROCESSING_STATUS_PAUSED =>
        EventingFunctionProcessingStatus.Paused
      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionProcessingStatus.PROCESSING_STATUS_RUNNING =>
        EventingFunctionProcessingStatus.Running
    }
  }

  private def convertToSdk(
      in: com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionDeploymentStatus
  ): EventingFunctionDeploymentStatus = {
    in match {
      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionDeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED =>
        EventingFunctionDeploymentStatus.Deployed
      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionDeploymentStatus.DEPLOYMENT_STATUS_UNDEPLOYED =>
        EventingFunctionDeploymentStatus.Undeployed
    }
  }

  private def convertBucketBindingsToSdk(
      in: Iterable[
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionBucketBinding
      ]
  ): Seq[EventingFunctionBucketBinding] = {
    in.map(bb =>
      EventingFunctionBucketBinding(
        bb.getAlias,
        convertToSdk(bb.getName),
        convertToSdk(bb.getAccess)
      )
    ).toSeq
  }

  private def convertConstantBindingsToSdk(
      in: Iterable[
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionConstantBinding
      ]
  ): Seq[EventingFunctionConstantBinding] = {
    in.map(bb => EventingFunctionConstantBinding(bb.getAlias, bb.getLiteral)).toSeq
  }

  private def convertUrlBindingsToSdk(
      in: Iterable[
        com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionUrlBinding
      ]
  ): Seq[EventingFunctionUrlBinding] = {
    in.map(bb =>
      EventingFunctionUrlBinding(
        bb.getHostname,
        bb.getAlias,
        convertToSdk(bb.getAuth),
        bb.getAllowCookies,
        bb.getValidateSslCertificate
      )
    ).toSeq
  }

  private def convertToSdk(
      in: com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionUrlAuth
  ): EventingFunctionUrlAuth = {
    if (in.hasNoAuth) EventingFunctionUrlAuth.None
    else if (in.hasBasicAuth)
      EventingFunctionUrlAuth.Basic(in.getBasicAuth.getUsername, Some(in.getBasicAuth.getPassword))
    else if (in.hasBearerAuth) EventingFunctionUrlAuth.Bearer(in.getBearerAuth.getKey)
    else if (in.hasDigestAuth)
      EventingFunctionUrlAuth.Basic(
        in.getDigestAuth.getUsername,
        Some(in.getDigestAuth.getPassword)
      )
    else throw new RuntimeException("Unknown auth " + in)
  }

  private def convertToSdk(
      in: com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionBucketAccess
  ): EventingFunctionBucketAccess = {
    in match {
      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionBucketAccess.READ_ONLY =>
        EventingFunctionBucketAccess.ReadOnly
      case com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionBucketAccess.READ_WRITE =>
        EventingFunctionBucketAccess.ReadWrite
    }
  }
}
