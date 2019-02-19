package com.couchbase.client.scala.kv

import com.couchbase.client.core.config.CouchbaseBucketConfig
import com.couchbase.client.core.error.CommonExceptions
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv.{GetAndLockRequest, GetAndLockResponse, GetRequest, ReplicaGetRequest}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.{HandlerParams, ReplicaMode}
import com.couchbase.client.scala.document.GetResult
import com.couchbase.client.scala.util.Validate
import io.opentracing.Span

import scala.util.{Failure, Success, Try}


class GetFromReplicaHandler(hp: HandlerParams) {

  def request[T](id: String,
                 replicaMode: ReplicaMode,
                 parentSpan: Option[Span] = None,
                 timeout: java.time.Duration,
                 retryStrategy: RetryStrategy)
  : Try[Seq[GetRequest]] = {
    val validations: Try[Seq[GetRequest]] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(replicaMode, "replicaMode")
      _ <- Validate.notNull(parentSpan, "parentSpan")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
    } yield null

    if (validations.isFailure) {
      validations
    }
    else {
      hp.core.clusterConfig().bucketConfig(hp.bucketName) match {
        case config: CouchbaseBucketConfig =>
          val numReplicas = config.numberOfReplicas()

          if (replicaMode == ReplicaMode.First && numReplicas < 1) {
            Failure(CommonExceptions.getFromReplicaInvalidReplica(0, numReplicas))
          }
          else if (replicaMode == ReplicaMode.Second && numReplicas < 2) {
            Failure(CommonExceptions.getFromReplicaInvalidReplica(1, numReplicas))
          }
          else if (replicaMode == ReplicaMode.Third && numReplicas < 3) {
            Failure(CommonExceptions.getFromReplicaInvalidReplica(2, numReplicas))
          }
          else {
            val replicaRequests: Seq[ReplicaGetRequest] = Range(0, numReplicas)
              .map(replicaIndex => new ReplicaGetRequest(id,
                hp.collectionIdEncoded,
                timeout,
                hp.core.context(),
                hp.bucketName,
                retryStrategy,
                (replicaIndex + 1).shortValue()))

            val activeRequest = new GetRequest(id,
              hp.collectionIdEncoded,
              timeout,
              hp.core.context(),
              hp.bucketName,
              retryStrategy)

            val requests: Seq[GetRequest] = replicaMode match {
              case ReplicaMode.All | ReplicaMode.Any => activeRequest +: replicaRequests
              case ReplicaMode.First => Seq(replicaRequests(0))
              case ReplicaMode.Second => Seq(replicaRequests(1))
              case ReplicaMode.Third => Seq(replicaRequests(2))
            }

            Success(requests)
          }

        case _ =>
          Failure(CommonExceptions.getFromReplicaNotCouchbaseBucket)
      }
    }
  }
}