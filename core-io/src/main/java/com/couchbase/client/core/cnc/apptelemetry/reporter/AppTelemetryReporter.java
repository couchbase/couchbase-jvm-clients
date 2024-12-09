/*
 * Copyright 2025 Couchbase, Inc.
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

import com.couchbase.client.core.annotation.Stability;

import java.io.Closeable;
import java.net.URI;
import java.util.Set;

@Stability.Internal
public interface AppTelemetryReporter extends Closeable {
  AppTelemetryReporter NOOP = new AppTelemetryReporter() {
    @Override
    public void updateRemotes(Set<URI> eligibleRemotes) {
    }

    @Override
    public void close() {
    }
  };

  void updateRemotes(Set<URI> eligibleRemotes);

  void close();
}
