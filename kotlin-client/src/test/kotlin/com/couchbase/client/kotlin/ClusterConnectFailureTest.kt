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

package com.couchbase.client.kotlin

import com.couchbase.client.core.env.PasswordAuthenticator
import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.kotlin.env.ClusterEnvironment
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * Verifies a failed `connect` doesn't leak the SDK-created environment (JVMCBC-1789).
 */
internal class ClusterConnectFailureTest {

    @Test
    fun `management port failure shuts down owned environment`() {
        val env = ClusterEnvironment.builder().build()
        try {
            assertThrows<InvalidArgumentException> { connectToManagementPort(env, ownsEnv = true) }
            assertTrue(env.scheduler().isDisposed, "owned environment should be shut down")
        } finally {
            // A second shutdown times out, so only shut down if the fix didn't.
            if (!env.scheduler().isDisposed) env.shutdown()
        }
    }

    @Test
    fun `management port failure leaves external environment running`() {
        val env = ClusterEnvironment.builder().build()
        try {
            assertThrows<InvalidArgumentException> { connectToManagementPort(env, ownsEnv = false) }
            assertFalse(env.scheduler().isDisposed, "external environment belongs to the caller")
        } finally {
            env.shutdown()
        }
    }

    private fun connectToManagementPort(env: ClusterEnvironment, ownsEnv: Boolean): Cluster =
        // SDK doesn't like KV port to be 8091 or 18091
        Cluster.doConnect(
            "couchbase://127.0.0.1:8091",
            PasswordAuthenticator.create("Administrator", "password"),
            env,
            ownsEnv,
        )
}
