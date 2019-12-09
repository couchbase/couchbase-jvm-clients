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

package com.couchbase.client.scala.kv

import com.couchbase.client.core.error._
import com.couchbase.client.core.msg.Response
import com.couchbase.client.core.msg.kv.KeyValueRequest

private[scala] object DefaultErrors {
  def throwOnBadResult(
      request: KeyValueRequest[_ <: Response],
      response: Response
  ): CouchbaseException = {
    DefaultErrorUtil.keyValueStatusToException(request, response)
  }
}
