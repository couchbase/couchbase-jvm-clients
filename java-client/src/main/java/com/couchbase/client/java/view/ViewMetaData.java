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

import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.view.ViewChunkHeader;
import com.couchbase.client.java.json.JsonObject;

import java.util.Optional;

/**
 * Holds Metadata associated with a {@link ViewResult}.
 *
 * @since 3.0.0
 */
public class ViewMetaData {

    /**
     * If present, holds debug information for the response.
     */
    private final Optional<JsonObject> debug;

    /**
     * Holds the total amount of rows in the view.
     */
    private final long totalRows;

    /**
     * Creates a new {@link ViewMetaData}.
     *
     * @param debug debug information if available.
     * @param totalRows the total number of rows in the view.
     */
    ViewMetaData(final Optional<JsonObject> debug, final long totalRows) {
        this.debug = debug;
        this.totalRows = totalRows;
    }

    /**
     * Creates the {@link ViewMetaData} from the chunk header.
     *
     * @param header the chunk header.
     * @return the initialized {@link ViewMetaData}.
     */
    static ViewMetaData from(final ViewChunkHeader header) {
        return new ViewMetaData(
            header.debug().map(bytes -> Mapper.decodeInto(bytes, JsonObject.class)),
            header.totalRows()
        );
    }

    /**
     * If present, returns debug information of the view request.
     *
     * <p>This information is only present if the debug flag has been set on the view options in the first place. Note
     * that this is a costly operation and should only be used - as the name suggests - during debugging.</p>
     *
     * @return the debug information as a generic {@link JsonObject} if present.
     */
    public Optional<JsonObject> debug() {
        return debug;
    }

    /**
     * Returns the total number of rows in the view as presented by the server.
     *
     * @return the total number of rows.
     */
    public long totalRows() {
        return totalRows;
    }

    @Override
    public String toString() {
        return "ViewMeta{" +
            "totalRows=" + totalRows +
            ", debug=" + debug +
            '}';
    }
}
