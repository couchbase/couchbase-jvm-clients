package com.couchbase.client.scala.document

// TODO
case class JsonObject() {

}


object JsonObject {
  private val EMPTY = JsonObject.create()

  def empty(): JsonObject = EMPTY

  def create() = new JsonObject
}