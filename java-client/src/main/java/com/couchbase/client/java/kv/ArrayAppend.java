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

package com.couchbase.client.java.kv;

import com.couchbase.client.core.api.kv.CoreSubdocMutateCommand;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.java.codec.JsonSerializer;

import java.util.List;

/**
 * An intention to perform a SubDocument array append operation.
 *
 * @author Graham Pople
 * @since 1.0.0
 */
public class ArrayAppend extends MutateInSpec {

    private final String path;
    private final List<?> doc;
    private boolean xattr = false;
    private boolean expandMacro = false;
    private boolean createPath = false;

    ArrayAppend(String path, List<?> doc) {
        this.path = path;
        this.doc = doc;
    }

    /**
     * Sets that this is an extended attribute (xattr) field.
     * @return this, for chaining
     */
    public ArrayAppend xattr() {
        xattr = true;
        return this;
    }

    /**
     * Sets that this parent fields should be created automatically.
     * @return this, for chaining
     */
    public ArrayAppend createPath() {
        createPath = true;
        return this;
    }

    @Override
    public CoreSubdocMutateCommand toCore(final JsonSerializer serializer) {
        return new CoreSubdocMutateCommand(
            SubdocCommandType.ARRAY_PUSH_LAST,
            path,
            MutateInUtil.convertDocsToBytes(doc, serializer),
            createPath,
            xattr,
            expandMacro
        );
    }
}
