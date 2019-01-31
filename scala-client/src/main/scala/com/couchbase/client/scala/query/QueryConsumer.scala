package com.couchbase.client.scala.query

import com.couchbase.client.core.msg.query.QueryResponse
import com.couchbase.client.core.msg.query.QueryResponse.QueryEventSubscriber

import scala.collection.mutable.ArrayBuffer

class QueryConsumer extends QueryEventSubscriber {
  private[scala] val rows = ArrayBuffer.empty[QueryResponse.QueryEvent]

  override def onNext(row: QueryResponse.QueryEvent): Unit = {
    rows += row
  }

  override def onComplete(): Unit = {
  }
}
