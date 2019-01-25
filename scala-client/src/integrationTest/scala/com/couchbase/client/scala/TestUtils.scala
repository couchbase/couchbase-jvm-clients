package com.couchbase.client.scala

object TestUtils {
  def docId(idx: Int = 0): String = {
    val st = Thread.currentThread.getStackTrace
    st(2).getMethodName + "_" + idx
  }
}
