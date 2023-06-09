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
 * An intention to perform a SubDocument array insert operation.
 *
 * @author Graham Pople
 * @since 1.0.0
 */
public class ArrayInsert extends MutateInSpec {

    private final String path;
    private final List<?> doc;
    private boolean xattr = false;
    private boolean expandMacro = false;
    private boolean createPath = false;

    ArrayInsert(String path, List<?> doc) {
        this.path = path;
        this.doc = doc;
    }

    /**
     * Sets that this is an extended attribute (xattr) field.
     * @return this, for chaining
     */
    public ArrayInsert xattr() {
        xattr = true;
        return this;
    }

    /**
     * Requests that any absent parent objects be created automatically.
     *
     * @return this, for chaining
     * @deprecated Couchbase Server does not support the "create path" option for `ArrayInsert`
     * sub-document operations. Calling this method causes `mutateIn` to throw
     * {@link com.couchbase.client.core.error.CouchbaseException}. Please do not call this method.
     * <p>
     * If you want to create missing parent objects, please use {@link MutateInSpec#arrayPrepend}
     * or {@link MutateInSpec#arrayAppend} instead of {@link MutateInSpec#arrayInsert}.
     */
    @Deprecated
    public ArrayInsert createPath() {
        createPath = true;
        return this;
    }

    @Override
    public CoreSubdocMutateCommand toCore(JsonSerializer serializer) {
        return new CoreSubdocMutateCommand(
            SubdocCommandType.ARRAY_INSERT,
            path,
            MutateInUtil.convertDocsToBytes(doc, serializer),
            createPath,
            xattr,
            expandMacro
        );
    }
}
