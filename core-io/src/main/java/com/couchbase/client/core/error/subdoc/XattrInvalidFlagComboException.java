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
import com.couchbase.client.core.error.context.ErrorContext;

/**
 * Subdocument exception thrown when a macro has been requested when not using extended attributes.
 * <p>
 * This exception is deprecated because this case, while technically possible, should never be thrown in the first
 * place to the user level because the SDK must catch this exception. It is never being thrown at this point.
 */
@Deprecated
public class XattrInvalidFlagComboException extends CouchbaseException {

    public XattrInvalidFlagComboException(final ErrorContext ctx) {
        super("Need to specify xattr flag when requesting macro for path", ctx);
    }
}
