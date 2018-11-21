package com.couchbase.client.scala.query

import com.couchbase.client.scala.document.JsonObject

class N1qlResult[T] {
  def rows(): Iterable[T] = null
  def allRows(): List[T] = null
  def status(): String = null
  def requestId(): String = null
  def clientContextId(): String = null
  // TODO other params
}

case class N1lQueryRow(bytes: List[Byte]) {
  def value(): JsonObject = null
}

class N1qlQueryResult extends N1qlResult[N1lQueryRow]
