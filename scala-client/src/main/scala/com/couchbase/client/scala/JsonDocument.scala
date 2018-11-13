package com.couchbase.client.scala

  case class JsonObject()

object JsonObject {
  def empty() = new JsonObject
}

case class JsonDocument(id: String,
                        content: JsonObject,
                        cas: Long)

object JsonDocument {
  def create(id: String, content: JsonObject): JsonDocument = null
}