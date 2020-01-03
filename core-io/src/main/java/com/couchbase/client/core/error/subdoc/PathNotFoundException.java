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

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.context.SubDocumentErrorContext;

/**
 * Subdocument exception thrown when a path does not exist in the document.
 * The exact meaning of path existence depends on the operation and inputs.
 */
public class PathNotFoundException extends CouchbaseException {

    public PathNotFoundException(final SubDocumentErrorContext ctx) {
        super("Subdoc path not found in the document", ctx);
    }

    @Override
    public SubDocumentErrorContext context() {
        return (SubDocumentErrorContext) super.context();
    }
}
