package com.couchbase.client.scala

case class Document[T](id: String,
                       content: T,
                       cas: Long
                       // TODO expiry
                      ) {

}
