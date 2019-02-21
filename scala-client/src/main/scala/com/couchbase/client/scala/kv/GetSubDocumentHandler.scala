package com.couchbase.client.scala.kv

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.error.subdoc.SubDocumentException
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv._
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.api._
import com.couchbase.client.scala.codec.DocumentFlags
import com.couchbase.client.scala.document.LookupInResult
import com.couchbase.client.scala.util.Validate
import io.netty.util.CharsetUtil
import io.opentracing.Span

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}


/**
  * Handles requests and responses for KV SubDocument operations.
  *
  * @author Graham Pople
  */
class GetSubDocumentHandler(hp: HandlerParams) extends RequestHandler[SubdocGetResponse, LookupInResult] {
  private val ExpTime = "$document.exptime"

  // TODO support projections


  def request[T](id: String,
                 spec: Seq[LookupInSpec],
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

      // Put expiration on the end so it doesn't mess up indexing
      // Update: no, all xattr commands need to at start. But only support expiration with full doc anyway (to avoid
      // app accidentally going over 16 subdoc commands), so can put it here
      if (withExpiration) {
        commands.add(new SubdocGetRequest.Command(SubdocCommandType.GET, ExpTime, true))
      }

      spec.map {
        case x: Get => new SubdocGetRequest.Command(SubdocCommandType.GET, x.path, x.xattr)
        case x: GetFullDocument => new SubdocGetRequest.Command(SubdocCommandType.GET_DOC, "", false)
        case x: Exists => new SubdocGetRequest.Command(SubdocCommandType.EXISTS, x.path, x.xattr)
        case x: Count => new SubdocGetRequest.Command(SubdocCommandType.COUNT, x.path, x.xattr)
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
        val values: Seq[SubdocField] = response.values().asScala

        var exptime: Option[Duration] = None
        val fields = collection.mutable.Map.empty[String, SubdocField]

        values.foreach(value => {
          if (value.path() == ExpTime) {
            val str = new java.lang.String(value.value(), CharsetUtil.UTF_8)
            exptime = Some(Duration(str.toLong, TimeUnit.SECONDS))
          }
        })

        LookupInResult(id, values, DocumentFlags.Json, response.cas(), exptime)

      case ResponseStatus.SUBDOC_FAILURE =>

        response.error().asScala match {
          case Some(err) => throw err
          case _ => throw new SubDocumentException("Unknown SubDocument failure occurred") {}
        }

      case _ => throw DefaultErrors.throwOnBadResult(response.status())
    }
  }

  def response(id: String, response: SubdocGetResponse, project: Seq[String]): LookupInResult = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        val values: Seq[SubdocField] = response.values().asScala

        var exptime: Option[Duration] = None
        val fields = collection.mutable.Map.empty[String, SubdocField]

        values.foreach(value => {
          if (value.path() == ExpTime) {
            val str = new java.lang.String(value.value(), CharsetUtil.UTF_8)
            exptime = Some(Duration(str.toLong, TimeUnit.SECONDS))
          }
        })

//        values.foreach(v => {
//          val parsed = JsonPathParser.parse(v.path())
//
//          parsed.foreach()
//        })
//        project.foreach(p => {
//
//        })

        LookupInResult(id, values, DocumentFlags.Json, response.cas(), exptime)

      case ResponseStatus.SUBDOC_FAILURE =>

        response.error().asScala match {
          case Some(err) => throw err
          case _ => throw new SubDocumentException("Unknown SubDocument failure occurred") {}
        }

      case _ => throw DefaultErrors.throwOnBadResult(response.status())
    }
  }
}