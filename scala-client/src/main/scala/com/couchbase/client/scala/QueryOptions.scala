package com.couchbase.client.scala

import java.util.Objects

import scala.concurrent.duration.Duration

case class Credential(login: String, password: String)

object N1qlProfile extends Enumeration {
  val Off, Phases, Timing = Value
}

//case class ScanVector(value: Int, guard: String)

sealed trait ScanConsistency
case class NotBounded() extends ScanConsistency
case class AtPlus(consistentWith: List[MutationToken], scanWait: Option[Duration] = None) extends ScanConsistency
case class RequestPlus(scanWait: Option[Duration] = None) extends ScanConsistency
case class StatementPlus(scanWait: Option[Duration] = None) extends ScanConsistency

case class MutationToken()

case class QueryOptions(namedParameters: Option[Map[String,Any]] = None,
                        positionalParameters: Option[List[Any]] = None,
                        contextId: Option[String] = None,
                        // TODO remove?
                        pretty: Option[Boolean] = None,
                        credentials: Option[List[Credential]] = None,
                        maxParallelism: Option[Int] = None,
                        disableMetrics: Option[Boolean] = None,
                        pipelineBatch: Option[Int] = None,
                        pipelineCap: Option[Int] = None,
                        profile: Option[N1qlProfile.Value] = None,
                        readonly: Option[Boolean] = None,
                        scanCap: Option[Int] = None,
                        scanConsistency: Option[ScanConsistency] = None,
//                        consistentWith: Option[List[MutationToken]]
                       serverSideTimeout: Option[Duration] = None,
                        timeout: Option[Duration] = None
                       ) {
  def namedParameter(name: String, value: Any): QueryOptions = {
    Objects.requireNonNull(name)
    Objects.requireNonNull(value)
    copy(namedParameters = Some(namedParameters.getOrElse(Map()) + (name -> value)))
  }

  def namedParameters(values: Map[String,Any]): QueryOptions = {
    Objects.requireNonNull(namedParameters)
    // TODO maybe can support "name" -> "value" syntax
    copy(namedParameters = Option(values))
  }

  def positionalParameters(values: Any*): QueryOptions = {
    Objects.requireNonNull(values)

    copy(positionalParameters = Option(values.toList))
  }

  def contextId(contextId: String): QueryOptions = {
    Objects.requireNonNull(contextId)
    copy(contextId = Option(contextId))
  }

  def pretty(pretty: Boolean): QueryOptions = {
    Objects.requireNonNull(pretty)
    copy(pretty = Option(pretty))
  }

  def credentials(credentials: List[Credential]): QueryOptions = {
    Objects.requireNonNull(credentials)
    copy(credentials = Option(credentials))
  }

  def credentials(login: String, password: String): QueryOptions = {
    Objects.requireNonNull(login)
    Objects.requireNonNull(password)
    copy(credentials = Option(List(Credential(login, password))))
  }

  def maxParallelism(maxParellism: Int): QueryOptions = {
    Objects.requireNonNull(maxParellism)
    copy(maxParallelism = Option(maxParellism))
  }

  def disableMetrics(disableMetrics: Boolean): QueryOptions = {
    Objects.requireNonNull(disableMetrics)
    copy(disableMetrics = Option(disableMetrics))
  }

  def profile(profile: N1qlProfile.Value): QueryOptions = {
    Objects.requireNonNull(profile)
    copy(profile = Option(profile))
  }

  def readonly(readonly: Boolean): QueryOptions= {
    Objects.requireNonNull(readonly)
    copy(readonly = Option(readonly))
  }

  def scanCap(scanCap: Int): QueryOptions = {
    Objects.requireNonNull(scanCap)
    copy(scanCap = Option(scanCap))
  }

  def scanConsistency(scanConsistency: ScanConsistency): QueryOptions = {
    Objects.requireNonNull(scanCap)
    copy(scanConsistency = Some(scanConsistency))
  }

//  def consistentWith(consistentWith: List[MutationToken]): QueryOptions = {
//    copy(consistentWith = Option(consistentWith))
//  }

  def serverSideTimeout(serverSideTimeout: Duration): QueryOptions = {
    Objects.requireNonNull(serverSideTimeout)
    copy(serverSideTimeout = Option(serverSideTimeout))
  }

  def timeout(timeout: Duration): QueryOptions = {
    Objects.requireNonNull(timeout)
    copy(timeout = Option(timeout))
  }
}


object QueryOptions {
  def apply() = new QueryOptions()
}