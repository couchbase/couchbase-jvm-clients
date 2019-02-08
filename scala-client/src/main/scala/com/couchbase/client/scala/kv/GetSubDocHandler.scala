package com.couchbase.client.scala.kv

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.error.subdoc.SubDocumentException
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv._
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.api._
import com.couchbase.client.scala.document.{DocumentFlags, LookupInResult}
import com.couchbase.client.scala.util.Validate
import io.netty.util.CharsetUtil
import io.opentracing.Span

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}


class GetSubDocHandler(hp: HandlerParams) extends RequestHandler[SubdocGetResponse, LookupInResult] {
  private val ExpTime = "$document.exptime"

  // TODO support projections
  // TODO support expiration (probs works, check unit tested)


  def request[T](id: String,
                 spec: LookupInSpec,
                 withExpiration: Boolean,
                 parentSpan: Option[Span],
                 timeout: java.time.Duration,
                 retryStrategy: RetryStrategy)
  : Try[SubdocGetRequest] = {
    val validations: Try[SubdocGetRequest] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(spec, "spec")
      _ <- Validate.notNull(parentSpan, "parentSpan")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
    } yield null

    if (validations.isFailure) {
      validations
    }
    else {
      val commands = new java.util.ArrayList[SubdocGetRequest.Command]()

      if (withExpiration) {
        commands.add(new SubdocGetRequest.Command(SubdocCommandType.GET, ExpTime, true))
      }

      spec.operations.map {
        case x: GetOperation => new SubdocGetRequest.Command(SubdocCommandType.GET, x.path, x.xattr)
        case x: GetFullDocumentOperation => new SubdocGetRequest.Command(SubdocCommandType.GET_DOC, "", false)
        case x: ExistsOperation => new SubdocGetRequest.Command(SubdocCommandType.EXISTS, x.path, x.xattr)
        case x: CountOperation => new SubdocGetRequest.Command(SubdocCommandType.COUNT, x.path, x.xattr)
      }.foreach(commands.add)


      if (commands.isEmpty) {
        Failure(new IllegalArgumentException("No SubDocument commands provided"))
      }
      else {
        Success(new SubdocGetRequest(timeout,
          hp.core.context(),
          hp.bucketName,
          retryStrategy,
          id,
          hp.collectionIdEncoded,
          0,
          commands))
      }
    }
  }

  def response(id: String, response: SubdocGetResponse): LookupInResult = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        val values = response.values().asScala

        var exptime: Option[FiniteDuration] = None
        var fulldoc: Option[Array[Byte]] = None
        val fields = collection.mutable.Map.empty[String, SubdocField]

        values.foreach(value => {
          if (value.path() == ExpTime) {
            val str = new java.lang.String(value.value(), CharsetUtil.UTF_8)
            exptime = Some(FiniteDuration(str.toLong, TimeUnit.SECONDS))
          }
          else if (value.path == "") {
            fulldoc = Some(value.value())
          }
          else {
            fields += value.path() -> value
          }
        })

        LookupInResult(id, fulldoc, fields, DocumentFlags.Json, response.cas(), exptime)

      case ResponseStatus.SUBDOC_FAILURE =>

        response.error().asScala match {
          case Some(err) => throw err
          case _ => throw new SubDocumentException("Unknown SubDocument failure occurred") {}
        }

      case _ => throw DefaultErrors.throwOnBadResult(response.status())
    }
  }
}