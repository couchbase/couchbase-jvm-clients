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

package com.couchbase.client.core.error.subdoc;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.context.SubDocumentErrorContext;

/**
 * Subdocument exception thrown when path is too deep to parse. Depth of a path is determined by how
 * many components (or levels) it contains.
 *
 * The current limitation is there to ensure a single parse does not consume too much memory (overloading the server).
 * This error is similar to other TooDeep errors, which all relate to various validation stages to ensure the server
 * does not consume too much memory when parsing a single document.
 */
public class PathTooDeepException extends CouchbaseException {

    public PathTooDeepException(final SubDocumentErrorContext ctx) {
        super("Subdoc path too deep", ctx);
    }

    @Override
    @Stability.Uncommitted
    public SubDocumentErrorContext context() {
        return (SubDocumentErrorContext) super.context();
    }
}
