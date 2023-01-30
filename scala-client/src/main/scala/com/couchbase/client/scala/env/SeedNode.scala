/*
 * Copyright (c) 2019 Couchbase, Inc.
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
package com.couchbase.client.scala.env
import scala.compat.java8.OptionConverters._

case class SeedNode(
    address: String,
    kvPort: Option[Int] = None,
    clusterManagerPort: Option[Int] = None,
    protostellarPort: Option[Int] = None
) {

  private[scala] def toCore: com.couchbase.client.core.env.SeedNode = {
    com.couchbase.client.core.env.SeedNode
      .create(address)
      .withKvPort(kvPort.map(Integer.valueOf).orNull)
      .withManagerPort(clusterManagerPort.map(Integer.valueOf).orNull)
      .withProtostellarPort(protostellarPort.map(Integer.valueOf).orNull)
  }
}

object SeedNode {
  private[scala] def fromCore(sn: com.couchbase.client.core.env.SeedNode): SeedNode = {
    SeedNode(
      sn.address,
      sn.kvPort.asScala.map(v => v.toInt),
      sn.clusterManagerPort.asScala.map(v => v.toInt),
      sn.protostellarPort.asScala.map(v => v.toInt)
    )
  }
}
