package com.couchbase.client.scala.kv

import com.couchbase.client.core.error.EncodingFailedException
import com.couchbase.client.core.error.subdoc.SubDocumentException
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv._
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.Validators
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.util.Validate
import io.opentracing.Span

import scala.compat.java8.OptionConverters._
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.compat.java8.OptionConverters._
import collection.JavaConverters._


/**
  * Handles requests and responses for KV SubDocument mutation operations.
  *
  * @author Graham Pople
  */
class MutateInHandler(hp: HandlerParams) extends RequestHandler[SubdocMutateResponse, MutateInResult] {

  def request[T](id: String,
                 spec: Seq[MutateInSpec],
                 cas: Long,
                 insertDocument: Boolean,
                 durability: Durability,
                 expiration: java.time.Duration,
                 parentSpan: Option[Span],
                 timeout: java.time.Duration,
                 retryStrategy: RetryStrategy)
  : Try[SubdocMutateRequest] = {
    val validations: Try[SubdocMutateRequest] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(cas, "cas")
      _ <- Validate.notNull(insertDocument, "insertDocument")
      _ <- Validate.notNull(durability, "durability")
      _ <- Validate.notNull(expiration, "expiration")
      _ <- Validate.notNull(parentSpan, "parentSpan")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
    } yield null

    if (validations.isFailure) {
      validations
    }
    else {
      // Find any decode failure
      val failed: Option[MutateInSpec] = spec
        .filter(_.isInstanceOf[MutateInSpecStandard])
        .find(v => v.asInstanceOf[MutateInSpecStandard].fragment.isFailure)

      failed match {
        case Some(failed: MutateInSpecStandard) =>
          // If any of the decodes failed, abort
          Failure(failed.fragment.failed.get)

        case _ =>

          val commands = new java.util.ArrayList[SubdocMutateRequest.Command]()
          spec.map(_.convert).foreach(commands.add)

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
  }

  def response(id: String, response: SubdocMutateResponse): MutateInResult = {
    response.status() match {

      case ResponseStatus.SUCCESS =>
        val values: Seq[SubdocField] = response.values().asScala

        MutateInResult(id, values, response.cas(), response.mutationToken().asScala)

      case ResponseStatus.SUBDOC_FAILURE =>

        response.error().asScala match {
          case Some(err) => throw err
          case _ => throw new SubDocumentException("Unknown SubDocument failure occurred") {}
        }

      case _ => throw DefaultErrors.throwOnBadResult(response.status())
    }
  }
}
