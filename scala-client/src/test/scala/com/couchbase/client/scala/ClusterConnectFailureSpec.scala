/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.scala

import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.core.util.ConnectionString
import com.couchbase.client.scala.env.{ClusterEnvironment, PasswordAuthenticator}
import org.junit.jupiter.api.Assertions.{assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

/** Verifies a failed `connect` doesn't leak the SDK-created environment (JVMCBC-1789). */
class ClusterConnectFailureSpec {

  @Test
  def managementPortFailureShutsDownOwnedEnvironment(): Unit = {
    val env = ClusterEnvironment.Builder(owned = true).build.get
    try {
      assertThrows(classOf[InvalidArgumentException], () => connectToManagementPort(env))
      assertTrue(env.coreEnv.scheduler.isDisposed, "owned environment should be shut down")
    } finally {
      env.shutdown()
    }
  }

  @Test
  def managementPortFailureLeavesExternalEnvironmentRunning(): Unit = {
    val env = ClusterEnvironment.Builder(owned = false).build.get
    try {
      assertThrows(classOf[InvalidArgumentException], () => connectToManagementPort(env))
      assertFalse(env.coreEnv.scheduler.isDisposed, "external environment belongs to the caller")
    } finally {
      env.shutdown()
    }
  }

  private def connectToManagementPort(env: ClusterEnvironment): AsyncCluster =
    // SDK doesn't like KV port to be 8091 or 18091
    new AsyncCluster(
      env,
      PasswordAuthenticator("Administrator", "password"),
      ConnectionString.create("couchbase://127.0.0.1:8091")
    )
}
