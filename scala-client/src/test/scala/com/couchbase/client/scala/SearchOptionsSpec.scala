/*
 * Copyright (c) 2021 Couchbase, Inc.
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

package com.couchbase.client.scala

import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.search.SearchOptions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import scala.concurrent.duration.Duration

class SearchOptionsSpec {

  @Test
  def appliesTimeout(): Unit = {
    val output = JsonObject.create
    SearchOptions().injectParams("idx", output, Duration("1s"))
    assertEquals(1000, output.obj("ctl").numLong("timeout"))
  }

  /** This is to preserve backwards compatibility with current code. */
  @Test
  def serverSideTimeoutOverrides(): Unit = {
    val output = JsonObject.create
    SearchOptions(serverSideTimeout = Some(Duration("3s")))
      .injectParams("idx", output, Duration("1s"))
    assertEquals(3000, output.obj("ctl").numLong("timeout"))
  }

}
