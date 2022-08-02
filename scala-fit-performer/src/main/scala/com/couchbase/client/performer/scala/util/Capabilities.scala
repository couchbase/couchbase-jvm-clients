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

import com.couchbase.client.protocol.sdk.Caps

import java.util

object Capabilities {
  def sdkImplementationCaps: util.List[Caps] = {
    val out = new util.ArrayList[Caps]

    // [start:3.1.5]
    out.add(Caps.SDK_PRESERVE_EXPIRY);
    // [end:3.1.5]

    out
  }

}
