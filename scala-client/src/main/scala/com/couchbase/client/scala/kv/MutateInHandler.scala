package com.couchbase.client.scala.kv

import com.couchbase.client.core.error.EncodingFailedException
import com.couchbase.client.core.error.subdoc.SubDocumentException
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv._
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.Validators
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.api.{MutateInSpec, MutateOperation, MutateOperationSimple, MutationResult}
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.document.MutateInResult
import com.couchbase.client.scala.durability.{Disabled, Durability}
import io.opentracing.Span

import scala.compat.java8.OptionConverters._
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.compat.java8.OptionConverters._
import collection.JavaConverters._


class MutateInHandler(hp: HandlerParams) extends RequestHandler[SubdocMutateResponse, MutateInResult] {

  def request[T](id: String,
                 spec: MutateInSpec,
                 cas: Long,
                 insertDocument: Boolean,
                 durability: Durability,
                 expiration: java.time.Duration,
                 parentSpan: Option[Span],
                 timeout: java.time.Duration,
                 retryStrategy: RetryStrategy)
  : Try[SubdocMutateRequest] = {
    Validators.notNullOrEmpty(id, "id")

    // Find any decode failure
    val failed: Option[MutateOperation] = spec.operations
      .filter(_.isInstanceOf[MutateOperationSimple])
      .find(v => v.asInstanceOf[MutateOperationSimple].fragment.isFailure)

    failed match {
      case Some(failed: MutateOperationSimple) =>
        // If any of the decodes failed, abort
        Failure(failed.fragment.failed.get)

      case _ =>

        val commands = new java.util.ArrayList[SubdocMutateRequest.Command]()
        spec.operations.map(_.convert).foreach(commands.add)

        if (commands.isEmpty) {
          Failure(SubdocMutateRequest.errIfNoCommands())
        }
        else if (commands.size > SubdocMutateRequest.SUBDOC_MAX_FIELDS) {
          Failure(SubdocMutateRequest.errIfNoCommands())
        }
        else {
          Success(new SubdocMutateRequest(timeout,
            hp.core.context(),
            hp.bucketName,
            retryStrategy,
            id,
            hp.collectionIdEncoded,
            insertDocument,
            commands,
            expiration.getSeconds,
            durability.toDurabilityLevel))
        }
    }
  }

  def response(id: String, response: SubdocMutateResponse): MutateInResult = {
    response.status() match {

      case ResponseStatus.SUCCESS =>
        val values: Seq[SubdocField] = response.values().asScala
        val fields: Map[String, SubdocField] = values.map(v => v.path() -> v).toMap

        MutateInResult(id, fields, response.cas(), response.mutationToken().asScala)

      case ResponseStatus.SUBDOC_FAILURE =>

        response.error().asScala match {
          case Some(err) => throw err
          case _ => throw new SubDocumentException("Unknown SubDocument failure occurred") {}
        }

      case _ => throw DefaultErrors.throwOnBadResult(response.status())
    }
  }
}
