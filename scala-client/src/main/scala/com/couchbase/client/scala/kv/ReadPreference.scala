/*
 * Copyright (c) 2025 Couchbase, Inc.
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
package com.couchbase.client.scala.kv

import com.couchbase.client.core.api.kv.CoreReadPreference

/** Controls which replicas are used for operations that support that. */
sealed trait ReadPreference {
  def toCore: CoreReadPreference
}

object ReadPreference {

  /** Uses only replicas in the preferred server group, which should be set with [[com.couchbase.client.scala.env.ClusterEnvironment.Builder.preferredServerGroup]] */
  case object PreferredServerGroup extends ReadPreference {
    override def toCore: CoreReadPreference = CoreReadPreference.PREFERRED_SERVER_GROUP
  }
}
