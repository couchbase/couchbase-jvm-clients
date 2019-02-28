package com.couchbase.client.scala.query

import com.couchbase.client.core.msg.query.QueryResponse
//import com.couchbase.client.core.msg.query.QueryResponse.{QueryEventSubscriber, QueryEventType}
//
//import scala.collection.mutable.ArrayBuffer
//
//class QueryConsumer extends QueryEventSubscriber {
//  private[scala] val rows = ArrayBuffer.empty[QueryResponse.QueryEvent]
//  private[scala] val errors = ArrayBuffer.empty[QueryResponse.QueryEvent]
//
//  override def onNext(row: QueryResponse.QueryEvent): Unit = {
//    if (row.rowType() == QueryEventType.ROW) rows += row
//    else if (row.rowType() == QueryEventType.ERROR) errors += row
//  }
//
//  override def onComplete(): Unit = {
//  }
//}
