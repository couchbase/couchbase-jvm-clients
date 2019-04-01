/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.error;

import java.nio.charset.StandardCharsets;

/**
 * There was a problem fulfilling the search request.
 *
 * Check <code>content()</code> for further details.
 *
 * @author Graham Pople
 * @since 2.0.0
 */
public class SearchServiceException extends CouchbaseException {
    private final byte[] content;

    public SearchServiceException(byte[] content) {
        super();
        this.content = content;
    }

    public byte[] content() {
        return content;
    }

    @Override
    public String getMessage() {
        return "Search Query Failed: " + new String(content, StandardCharsets.UTF_8);
    }
}
