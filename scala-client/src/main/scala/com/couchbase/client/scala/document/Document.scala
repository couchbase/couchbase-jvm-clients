package com.couchbase.client.scala.document


case class Document[T](id: String,
                       content: T,
                       cas: Long
                       // TODO expiry
                      // TODO mutation token?
                      ) {
}

class JsonDocument(id: String,
                        content: JsonObject,
                        cas: Long) extends Document[JsonObject](id, content, cas) {

}