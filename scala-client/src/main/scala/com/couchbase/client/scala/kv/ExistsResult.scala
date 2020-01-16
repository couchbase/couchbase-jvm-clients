package com.couchbase.client.scala.kv

/** The result of an `exists` operation.
  *
  * @param exists whether the document exists
  */
case class ExistsResult(exists: Boolean, cas: Long)
