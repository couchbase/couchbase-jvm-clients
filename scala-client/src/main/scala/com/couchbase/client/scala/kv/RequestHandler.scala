package com.couchbase.client.scala.kv

trait RequestHandler[Resp,Res] {
  def response(id: String, response: Resp): Res
}
