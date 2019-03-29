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

package com.couchbase.client.java.view;

import com.couchbase.client.java.json.JsonObject;

import java.util.Optional;

public class ViewMeta {

    private final Optional<JsonObject> debug;
    private final long totalRows;

    public ViewMeta(Optional<JsonObject> debug, long totalRows) {
        this.debug = debug;
        this.totalRows = totalRows;
    }

    public Optional<JsonObject> debug() {
        return debug;
    }

    public long totalRows() {
        return totalRows;
    }

    @Override
    public String toString() {
        return "ViewMeta{" +
            "debug=" + debug +
            ", totalRows=" + totalRows +
            '}';
    }
}
