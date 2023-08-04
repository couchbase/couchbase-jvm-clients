/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.config.refresher.GlobalRefresher;
import com.couchbase.client.core.config.refresher.KeyValueBucketRefresher;

public interface ConfigRequest {
  enum Purpose {
    /**
     * Fetching the config for the first time.
     */
    DISCOVERY,

    /**
     * Checking for a change to a known config.
     * <p>
     * When dispatched to a KV endpoint where clustermap change notifications are enabled,
     * a request with this purpose is instantly completed with an empty synthetic response,
     * because config polling is not needed for those endpoints.
     * <p>
     * As a result, {@link GlobalRefresher} and {@link KeyValueBucketRefresher} can dispatch
     * "get config" requests to all nodes, and the request is only be executed on nodes
     * where the modern change notification feature is not supported.
     */
    REFRESH,
  }

  Purpose purpose();
}
