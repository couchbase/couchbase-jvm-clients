/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.cnc.apptelemetry.reporter;

import java.net.URI;

/**
 * The connection {@link AppTelemetryReporterImpl} uses to report telemetry:
 * connects to an app telemetry server, and answers its requests for telemetry until the connection closes.
 */
interface AppTelemetryClient {

  /**
   * Connects to the given WebSocket URI, and blocks until the connection closes.
   *
   * @param doOnSuccessfulConnection called once the WebSocket handshake succeeds
   * @throws InterruptedException if the thread was interrupted
   * @throws RuntimeException if the connection attempt failed for any other reason
   */
  void connectAndWaitForClose(URI remote, Runnable doOnSuccessfulConnection) throws InterruptedException;
}
