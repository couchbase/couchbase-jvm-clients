package com.couchbase.client.scala.kv

sealed class Document
object Document {
  case object DoNothing extends Document
  case object Insert extends Document
  case object Upsert extends Document

}
