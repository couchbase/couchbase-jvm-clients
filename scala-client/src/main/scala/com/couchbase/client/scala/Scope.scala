package com.couchbase.client.scala

import com.couchbase.client.core.Core

// For now, use SDK 2 Cluster & Bucket objects as our base layer for SDK 3 prototyping
class Scope(val core: Core,
             val cluster: Cluster,
            val bucket: Bucket,
            val name: String) {
  def openCollection(name: String): Collection = {
    new Collection(name, this)
  }
}

