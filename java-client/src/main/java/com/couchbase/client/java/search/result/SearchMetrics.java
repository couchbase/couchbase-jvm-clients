/*
 * Copyright (c) 2016 Couchbase, Inc.
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
package com.couchbase.client.java.search.result;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.result.CoreSearchMetrics;

import java.time.Duration;

public class SearchMetrics {

    private final CoreSearchMetrics internal;

    @Stability.Internal
    public SearchMetrics(CoreSearchMetrics internal) {
        this.internal = internal;
    }

    public Duration took() {
        return internal.took();
    }

    public long totalRows() {
        return internal.totalRows();
    }

    public double maxScore() {
        return internal.maxScore();
    }

    public long successPartitionCount() {
        return internal.successPartitionCount();
    }

    public long errorPartitionCount() {
        return internal.errorPartitionCount();
    }

    public long totalPartitionCount() {
        return internal.totalPartitionCount();
    }

    @Override
    public String toString() {
        return internal.toString();
    }
}
