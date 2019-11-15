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

import com.couchbase.client.core.error.ErrorContext;

/**
 * Subdocument exception thrown when the provided value cannot be inserted at the given path.
 *
 * It is actually thrown when the delta in an counter operation is valid, but applying that delta would
 * result in an out-of-range number (over {@link Long#MAX_VALUE} or under {@link Long#MIN_VALUE}).
 *
 * @author Simon Baslé
 * @since 2.0
 */
public class CannotInsertValueException extends SubDocumentException {

    public CannotInsertValueException(ErrorContext ctx, int index) {
        super("Cannot insert the subdoc value", ctx, index);
    }

}
