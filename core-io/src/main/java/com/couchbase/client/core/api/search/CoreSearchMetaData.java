/*
 * Copyright (c) 2023 Couchbase, Inc.
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

package com.couchbase.client.core.api.search;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.result.CoreSearchMetrics;

import java.util.Map;

@Stability.Internal
public class CoreSearchMetaData {
    private final Map<String, String> errors;
    private final CoreSearchMetrics metrics;

    CoreSearchMetaData(final Map<String, String> errors, final CoreSearchMetrics metrics) {
        this.errors = errors;
        this.metrics = metrics;
    }

    public CoreSearchMetrics metrics() {
        return metrics;
    }

    public Map<String, String> errors() {
        return errors;
    }

    @Override
    public String toString() {
        return "SearchMetaData{" +
          "metrics=" + metrics +
          ", errors=" + errors +
          '}';
    }
}
