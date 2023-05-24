/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.scala.manager.eventing

import com.couchbase.client.core.{Core, CoreProtostellar}
import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.api.CoreCouchbaseOps
import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.endpoint.http.CoreCommonOptions
import com.couchbase.client.core.error.DecodingFailureException
import com.couchbase.client.core.manager.CoreEventingFunctionManager
import com.couchbase.client.core.protostellar.CoreProtostellarUtil
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.json.{JsonArray, JsonArraySafe, JsonObject, JsonObjectSafe}
import com.couchbase.client.scala.query.QueryScanConsistency
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.{FunctionalUtil, FutureConversions}

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

@Stability.Uncommitted
class AsyncEventingFunctionManager(
    private val env: ClusterEnvironment,
    private val couchbaseOps: CoreCouchbaseOps
)(
    implicit ec: ExecutionContext
) {
  private[scala] val DefaultTimeout       = env.timeoutConfig.managementTimeout
  private[scala] val DefaultRetryStrategy = env.retryStrategy

  private def coreManagerTry: Future[CoreEventingFunctionManager] = {
    couchbaseOps match {
      case core: Core => Future.successful(new CoreEventingFunctionManager(core))
      case _ =>
        Future.failed(
          CoreProtostellarUtil.unsupportedInProtostellar("eventing function management")
        )
    }
  }

  def upsertFunction(
      function: EventingFunction,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Future[Unit] = {
    coreManagerTry.flatMap(
      coreManager =>
        FutureConversions
          .javaCFToScalaFutureMappingExceptions(
            coreManager.upsertFunction(
              function.name,
              AsyncEventingFunctionManager.encodeFunction(function),
              makeOptions(timeout, retryStrategy, parentSpan)
            )
          )
          .map(_ => ())
    )
  }

  def getFunction(
      name: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Future[EventingFunction] = {
    coreManagerTry.flatMap(
      coreManager =>
        FutureConversions
          .javaCFToScalaFutureMappingExceptions(
            coreManager.getFunction(name, makeOptions(timeout, retryStrategy, parentSpan))
          )
          .flatMap(
            v =>
              AsyncEventingFunctionManager.decodeFunction(v) match {
                case Success(x)   => Future.successful(x)
                case Failure(err) => Future.failed(err)
              }
          )
    )
  }

  def dropFunction(
      name: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Future[Unit] = {
    coreManagerTry.flatMap(
      coreManager =>
        FutureConversions
          .javaCFToScalaFutureMappingExceptions(
            coreManager.dropFunction(name, makeOptions(timeout, retryStrategy, parentSpan))
          )
          .map(_ => ())
    )
  }

  private def makeOptions(
      timeout: Duration,
      retryStrategy: RetryStrategy,
      parentSpan: Option[RequestSpan]
  ): CoreCommonOptions = {
    CoreCommonOptions.of(timeout, retryStrategy, parentSpan.orNull)
  }

  def deployFunction(
      name: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Future[Unit] = {
    coreManagerTry.flatMap(
      coreManager =>
        FutureConversions
          .javaCFToScalaFutureMappingExceptions(
            coreManager.deployFunction(name, makeOptions(timeout, retryStrategy, parentSpan))
          )
          .map(_ => ())
    )
  }

  def getAllFunctions(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Future[Seq[EventingFunction]] = {
    coreManagerTry.flatMap(
      coreManager =>
        FutureConversions
          .javaCFToScalaFutureMappingExceptions(
            coreManager
              .getAllFunctions(makeOptions(timeout, retryStrategy, parentSpan))
          )
          .flatMap(
            v =>
              AsyncEventingFunctionManager.decodeFunctions(v) match {
                case Success(x)   => Future.successful(x)
                case Failure(err) => Future.failed(err)
              }
          )
    )
  }

  def pauseFunction(
      name: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Future[Unit] = {
    coreManagerTry.flatMap(
      coreManager =>
        FutureConversions
          .javaCFToScalaFutureMappingExceptions(
            coreManager.pauseFunction(name, makeOptions(timeout, retryStrategy, parentSpan))
          )
          .map(_ => ())
    )
  }

  def resumeFunction(
      name: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Future[Unit] = {
    coreManagerTry.flatMap(
      coreManager =>
        FutureConversions
          .javaCFToScalaFutureMappingExceptions(
            coreManager.resumeFunction(name, makeOptions(timeout, retryStrategy, parentSpan))
          )
          .map(_ => ())
    )
  }

  def undeployFunction(
      name: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Future[Unit] = {
    coreManagerTry.flatMap(
      coreManager =>
        FutureConversions
          .javaCFToScalaFutureMappingExceptions(
            coreManager.undeployFunction(name, makeOptions(timeout, retryStrategy, parentSpan))
          )
          .map(_ => ())
    )
  }

  def functionsStatus(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Future[EventingStatus] = {
    coreManagerTry.flatMap(
      coreManager =>
        FutureConversions
          .javaCFToScalaFutureMappingExceptions(
            coreManager
              .functionsStatus(makeOptions(timeout, retryStrategy, parentSpan))
          )
          .flatMap(
            bytes =>
              JsonObjectSafe
                .fromJsonSafe(new String(bytes, StandardCharsets.UTF_8))
                .flatMap(json => AsyncEventingFunctionManager.decodeStatus(json)) match {
                case Success(x)   => Future.successful(x)
                case Failure(err) => Future.failed(err)
              }
          )
    )
  }
}

object AsyncEventingFunctionManager {
  private def encodeFunction(function: EventingFunction): Array[Byte] = {
    val func = JsonObject.create

    func.put("appname", function.name)
    func.put("appcode", function.code)
    function.version.foreach(v => func.put("version", v))
    function.enforceSchema.foreach(v => func.put("enforce_schema", v))
    function.handlerUuid.foreach(v => func.put("handleruuid", v))
    function.functionInstanceId.foreach(v => func.put("function_instance_id", v))

    val depcfg = JsonObject.create

    depcfg.put("source_bucket", function.sourceKeyspace.bucket)
    function.sourceKeyspace.scope.foreach(v => depcfg.put("source_scope", v))
    function.sourceKeyspace.collection.foreach(v => depcfg.put("source_collection", v))
    depcfg.put("metadata_bucket", function.metadataKeyspace.bucket)
    function.metadataKeyspace.scope.foreach(v => depcfg.put("metadata_scope", v))
    function.metadataKeyspace.collection.foreach(v => depcfg.put("metadata_collection", v))

    function.constantBindings match {
      case Some(cb) if cb.nonEmpty =>
        val constants = JsonArray.create
        cb.foreach(v => {
          constants.add(
            JsonObject.create
              .put("alias", v.alias)
              .put("literal", v.literal)
          )
        })
        depcfg.put("constants", constants)
      case _ =>
    }

    function.urlBindings match {
      case Some(bindings) if bindings.nonEmpty =>
        val urls = JsonArray.create
        bindings.foreach(c => {
          val map = JsonObject.create
          map.put("alias", c.alias)
          map.put("hostname", c.hostname)
          map.put("allow_cookies", c.allowCookies)
          map.put("validate_ssl_certificates", c.validateSslCertificate)

          c.auth match {
            case EventingFunctionUrlAuth.None =>
              map.put("auth_type", "no-auth")
            case v: EventingFunctionUrlAuth.Basic =>
              map.put("auth_type", "basic")
              map.put("username", v.username)
              v.password.foreach(x => map.put("password", x))
            case v: EventingFunctionUrlAuth.Digest =>
              map.put("auth_type", "digest")
            case v: EventingFunctionUrlAuth.Bearer =>
              map.put("auth_type", "bearer")
          }
          urls.add(map)
        })
        depcfg.put("curl", urls)
      case _ =>
    }

    function.bucketBindings match {
      case Some(bindings) if bindings.nonEmpty =>
        val buckets = JsonArray.create
        bindings.foreach(c => {
          val map = JsonObject.create
          map.put("alias", c.alias)
          map.put("bucket_name", c.name.bucket)
          map.put("scope_name", c.name.scope)
          map.put("collection_name", c.name.collection)
          val access = c.access match {
            case EventingFunctionBucketAccess.ReadOnly  => "r"
            case EventingFunctionBucketAccess.ReadWrite => "rw"
          }
          map.put("access", access)
          buckets.add(map)
        })
        depcfg.put("buckets", buckets)
      case _ =>
    }

    val settings = JsonObject.create
    function.settings match {
      case Some(efs) =>
        efs.processingStatus match {
          case Some(EventingFunctionProcessingStatus.Running) =>
            settings.put("processing_status", true)
          case _ => settings.put("processing_status", false)
        }
        efs.deploymentStatus match {
          case Some(EventingFunctionDeploymentStatus.Deployed) =>
            settings.put("deployment_status", true)
          case _ => settings.put("deployment_status", false)
        }

        efs.cppWorkerThreadCount.foreach(v => settings.put("cpp_worker_thread_count", v))
        efs.dcpStreamBoundary.foreach(
          v =>
            settings.put("dcp_stream_boundary", v match {
              case EventingFunctionDcpBoundary.Everything => "everything"
              case EventingFunctionDcpBoundary.FromNow    => "from_now"
            })
        )
        efs.description.foreach(v => settings.put("description", v))
        efs.logLevel.foreach(
          v =>
            settings.put(
              "log_level",
              v match {
                case EventingFunctionLogLevel.Info    => "INFO"
                case EventingFunctionLogLevel.Error   => "ERROR"
                case EventingFunctionLogLevel.Warning => "WARN"
                case EventingFunctionLogLevel.Debug   => "DEBUG"
                case EventingFunctionLogLevel.Trace   => "TRACE"
              }
            )
        )
        efs.languageCompatibility.foreach(
          v =>
            settings.put(
              "language_compatibility",
              v match {
                case EventingFunctionLanguageCompatibility.Version_6_0_0 => "6.0.0"
                case EventingFunctionLanguageCompatibility.Version_6_5_0 => "6.5.0"
                case EventingFunctionLanguageCompatibility.Version_6_6_2 => "6.6.2"
              }
            )
        )
        efs.executionTimeout.foreach(v => settings.put("execution_timeout", v.toSeconds))
        efs.lcbTimeout.foreach(v => settings.put("lcb_timeout", v.toSeconds))
        efs.lcbInstCapacity.foreach(v => settings.put("lcb_inst_capacity", v))
        efs.lcbRetryCount.foreach(v => settings.put("lcb_retry_count", v))
        efs.numTimerPartitions.foreach(v => settings.put("num_timer_partitions", v))
        efs.sockBatchSize.foreach(v => settings.put("sock_batch_size", v))
        efs.tickDuration.foreach(v => settings.put("tick_duration", v.toMillis))
        efs.timerContextSize.foreach(v => settings.put("timer_context_size", v))
        efs.bucketCacheSize.foreach(v => settings.put("bucket_cache_size", v))
        efs.bucketCacheAge.foreach(v => settings.put("bucket_cache_age", v))
        efs.curlMaxAllowedRespSize.foreach(v => settings.put("curl_max_allowed_resp_size", v))
        efs.workerCount.foreach(v => settings.put("worker_count", v))
        efs.appLogMaxSize.foreach(v => settings.put("app_log_max_size", v))
        efs.appLogMaxFiles.foreach(v => settings.put("app_log_max_files", v))
        efs.checkpointInterval.foreach(v => settings.put("checkpoint_interval", v.toSeconds))
        efs.handlerHeaders match {
          case Some(v) if v.nonEmpty => settings.put("handler_headers", JsonArray.fromSeq(v))
          case _                     =>
        }
        efs.handlerFooters match {
          case Some(v) if v.nonEmpty => settings.put("handler_fotters", JsonArray.fromSeq(v))
          case _                     =>
        }
        efs.queryPrepareAll.foreach(v => settings.put("n1ql_prepare_all", v))
        efs.enableAppLogRotation.foreach(v => settings.put("enable_applog_rotation", v))
        efs.userPrefix.foreach(v => settings.put("user_prefix", v))
        efs.appLogDir.foreach(v => settings.put("app_log_dir", v))
        efs.queryConsistency match {
          case Some(_: QueryScanConsistency.RequestPlus) =>
            settings.put("n1ql_consistency", "request")
          case _ => settings.put("n1ql_consistency", "none")
        }
      case _ =>
        settings.put("processing_status", false)
        settings.put("deployment_status", false)
    }

    func.put("depcfg", depcfg)
    func.put("settings", settings)

    func.toString.getBytes(StandardCharsets.UTF_8)
  }

  def decodeStatus(in: JsonObjectSafe): Try[EventingStatus] = {
    in.num("num_eventing_nodes")
      .flatMap(numEventingNodes => {
        in.arr("apps")
          .flatMap(apps => {
            val functions = apps.iterator.map {
              case app: JsonObjectSafe =>
                for {
                  name   <- app.str("name")
                  status <- app.str("composite_status")
                  statusMapped <- status match {
                    case "undeployed"  => Success(EventingFunctionStatus.Undeployed)
                    case "deploying"   => Success(EventingFunctionStatus.Deploying)
                    case "deployed"    => Success(EventingFunctionStatus.Deployed)
                    case "undeploying" => Success(EventingFunctionStatus.Deploying)
                    case "paused"      => Success(EventingFunctionStatus.Paused)
                    case "pausing"     => Success(EventingFunctionStatus.Pausing)
                    case _ =>
                      Failure(new DecodingFailureException(s"Unknown composite_status ${status}"))
                  }
                  numBootstrappingNodes <- app.num("num_bootstrapping_nodes")
                  numDeployedNodes      <- app.num("num_deployed_nodes")
                  deploymentStatus <- app.bool("deployment_status") match {
                    case Success(true) => Success(EventingFunctionDeploymentStatus.Deployed)
                    case _             => Success(EventingFunctionDeploymentStatus.Undeployed)
                  }
                  processingStatus <- app.bool("processing_status") match {
                    case Success(true) => Success(EventingFunctionProcessingStatus.Running)
                    case _             => Success(EventingFunctionProcessingStatus.Paused)
                  }
                } yield EventingFunctionState(
                  name,
                  statusMapped,
                  numBootstrappingNodes,
                  numDeployedNodes,
                  deploymentStatus,
                  processingStatus
                )
            }.toSeq
            FunctionalUtil.traverse(functions).map(v => EventingStatus(numEventingNodes, v))
          })
      })
  }

  def decodeKeyspace(in: JsonObjectSafe, prefix: String): Try[EventingFunctionKeyspace] = {
    val bucket     = in.str(prefix + "bucket")
    val scope      = in.str(prefix + "scope").toOption
    val collection = in.str(prefix + "collection").toOption
    val x          = bucket.map(b => EventingFunctionKeyspace(b, scope, collection))
    x
  }

  def decodeFunction(encoded: Array[Byte]): Try[EventingFunction] = {
    val s = new String(encoded, StandardCharsets.UTF_8)
    JsonObjectSafe
      .fromJsonSafe(s)
      .flatMap(func => {
        for {
          depcfg           <- func.obj("depcfg")
          settings         <- func.obj("settings")
          appname          <- func.str("appname")
          appcode          <- func.str("appcode")
          sourceKeyspace   <- decodeKeyspace(depcfg, "source_")
          metadataKeyspace <- decodeKeyspace(depcfg, "metadata_")
        } yield {

          val s = EventingFunctionSettings(
            settings.numLong("cpp_worker_thread_count").toOption,
            settings.str("dcp_stream_boundary") match {
              case Success("everything") => Some(EventingFunctionDcpBoundary.Everything)
              case Success("from_now")   => Some(EventingFunctionDcpBoundary.FromNow)
              case _                     => None
            },
            settings.str("description").toOption,
            settings.str("log_level") match {
              case Success("ERROR")   => Some(EventingFunctionLogLevel.Error)
              case Success("WARNING") => Some(EventingFunctionLogLevel.Warning)
              case Success("INFO")    => Some(EventingFunctionLogLevel.Info)
              case Success("DEBUG")   => Some(EventingFunctionLogLevel.Debug)
              case Success("TRACE")   => Some(EventingFunctionLogLevel.Trace)
              case _                  => None
            },
            settings.str("language_compatibility") match {
              case Success("6.0.0") => Some(EventingFunctionLanguageCompatibility.Version_6_0_0)
              case Success("6.5.0") => Some(EventingFunctionLanguageCompatibility.Version_6_5_0)
              case Success("6.6.2") => Some(EventingFunctionLanguageCompatibility.Version_6_6_2)
              case _                => None
            },
            settings
              .numLong("execution_timeout")
              .toOption
              .map(v => Duration.create(v, TimeUnit.SECONDS)),
            settings.numLong("lcb_inst_capacity").toOption,
            settings.numLong("lcb_retry_count").toOption,
            settings.numLong("lcb_timeout").toOption.map(v => Duration.create(v, TimeUnit.SECONDS)),
            settings.str("n1ql_consistency") match {
              case Success("request") => Some(QueryScanConsistency.RequestPlus())
              case _                  => None
            },
            settings.numLong("num_timer_partitions").toOption,
            settings.numLong("sock_batch_size").toOption,
            settings
              .numLong("tick_duration")
              .toOption
              .map(v => Duration.create(v, TimeUnit.MILLISECONDS)),
            settings.numLong("timer_context_size").toOption,
            settings.str("user_prefix").toOption,
            settings.numLong("bucket_cache_size").toOption,
            settings.numLong("bucket_cache_age").toOption,
            settings.numLong("curl_max_allowed_resp_size").toOption,
            settings.numLong("worker_count").toOption,
            settings.bool("n1ql_prepare_all").toOption,
            settings.arr("handler_headers").toOption.map(v => v.toSeq.map(x => x.toString)),
            settings.arr("handler_footers").toOption.map(v => v.toSeq.map(x => x.toString)),
            settings.bool("enable_applog_rotation").toOption,
            settings.str("app_log_dir").toOption,
            settings.numLong("app_log_max_size").toOption,
            settings.numLong("app_log_max_files").toOption,
            settings
              .numLong("checkpoint_interval")
              .toOption
              .map(v => Duration.create(v, TimeUnit.SECONDS)),
            settings.bool("processing_status") match {
              case Success(true)  => Some(EventingFunctionProcessingStatus.Running)
              case Success(false) => Some(EventingFunctionProcessingStatus.Paused)
              case _              => None
            },
            settings.bool("deployment_status") match {
              case Success(true)  => Some(EventingFunctionDeploymentStatus.Deployed)
              case Success(false) => Some(EventingFunctionDeploymentStatus.Undeployed)
              case _              => None
            }
          )

          val bucketBindings: Option[Seq[EventingFunctionBucketBinding]] =
            depcfg.arr("buckets") match {
              case Success(ja) =>
                Some(ja.iterator.map {
                  case v: JsonObjectSafe =>
                    EventingFunctionBucketBinding(
                      v.str("alias").get,
                      EventingFunctionKeyspace(
                        v.str("bucket_name").get,
                        v.str("scope_name").toOption,
                        v.str("collection_name").toOption
                      ),
                      v.str("access") match {
                        case Success("rw") => EventingFunctionBucketAccess.ReadWrite
                        case _             => EventingFunctionBucketAccess.ReadOnly
                      }
                    )
                }.toSeq)
              case _ => None
            }

          val constantBindings: Option[Seq[EventingFunctionConstantBinding]] =
            depcfg.arr("constants") match {
              case Success(ja) =>
                Some(ja.iterator.map {
                  case v: JsonObjectSafe =>
                    EventingFunctionConstantBinding(v.str("value").get, v.str("literal").get)
                }.toSeq)
              case _ => None
            }

          val urlBindings: Option[Seq[EventingFunctionUrlBinding]] = depcfg.arr("curl") match {
            case Success(ja) =>
              Some(ja.iterator.map {
                case v: JsonObjectSafe =>
                  EventingFunctionUrlBinding(
                    v.str("hostname").get,
                    v.str("alias").get,
                    v.str("auth_type") match {
                      case Success("basic") =>
                        EventingFunctionUrlAuth.Basic(v.str("username").get, None)
                      case Success("digest") =>
                        EventingFunctionUrlAuth.Digest(v.str("username").get, None)
                      case Success("bearer") =>
                        EventingFunctionUrlAuth.Bearer(v.str("bearer_key").get)
                      case _ => EventingFunctionUrlAuth.None
                    },
                    v.bool("allow_cookies").getOrElse(false),
                    v.bool("validate_ssl_certificate").getOrElse(false)
                  )
              }.toSeq)
            case _ => None
          }

          EventingFunction(
            appname,
            appcode,
            sourceKeyspace,
            metadataKeyspace,
            Some(s),
            func.str("version").toOption,
            func.bool("enforce_schema").toOption,
            func.numLong("handleruuid").toOption,
            func.str("function_instance_id").toOption,
            bucketBindings,
            urlBindings,
            constantBindings
          )
        }
      })
  }

  def decodeFunctions(encoded: Array[Byte]): Try[Seq[EventingFunction]] = {
    val s = new String(encoded, StandardCharsets.UTF_8)
    JsonArraySafe.fromJsonSafe(s) match {
      case Success(j) =>
        val x = j.iterator.map {
          case v: JsonObjectSafe =>
            val reencoded = v.toString
            decodeFunction(reencoded.getBytes(StandardCharsets.UTF_8))
        }.toSeq
        FunctionalUtil.traverse(x)

      case Failure(err) => Failure(err)
    }
  }
}
