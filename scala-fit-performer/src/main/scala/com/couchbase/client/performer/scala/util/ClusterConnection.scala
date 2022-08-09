/*
 * Copyright (c) 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.performer.scala.util

import com.couchbase.client.protocol.shared.{ClusterConnectionCreateRequest, DocLocation}
import com.couchbase.client.scala.{Bucket, Cluster, Collection}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

class ClusterConnection(req: ClusterConnectionCreateRequest) {
  private val logger = LoggerFactory.getLogger(classOf[ClusterConnection])
  private val hostname = "couchbase://" + req.getClusterHostname
  logger.info("Attempting connection to cluster " + hostname)

  val cluster = Cluster.connect(hostname, req.getClusterUsername, req.getClusterPassword).get
  private val bucketCache = scala.collection.mutable.Map.empty[String, Bucket]

  // SCBC-365: hit performance problems when repeatedly opening a scope or collection.  99% of the time we'll be
  // using the same collection every time, so for performance don't use a map here
  // Have to store both these as very old versions of the SDK don't put bucketName and scopeName on Collection
  private var lastCollectionGrpc: com.couchbase.client.protocol.shared.Collection = _
  private var lastCollection: Collection = _

  cluster.waitUntilReady(30.seconds)

  def collection(loc: DocLocation): Collection = {
    val coll = {
      if (loc.hasPool) loc.getPool.getCollection
      else if (loc.hasSpecific) loc.getSpecific.getCollection
      else if (loc.hasUuid) loc.getUuid.getCollection
      else throw new UnsupportedOperationException("Unknown DocLocation type")
    }

    if (lastCollectionGrpc == coll) {
      lastCollection
    }
    else {
      val bucket = bucketCache.getOrElseUpdate(coll.getBucketName, {
        logger.info(s"Opening new bucket ${coll.getBucketName}")
        cluster.bucket(coll.getBucketName)
      })
      val out = bucket.scope(coll.getScopeName).collection(coll.getCollectionName)
      lastCollectionGrpc = coll
      lastCollection = out
      out
    }
  }
}
