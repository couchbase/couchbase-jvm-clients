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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.java.codec.JsonSerializer;

import static com.couchbase.client.core.util.Validators.notNull;

/**
 * An intention to perform a SubDocument insert operation.
 *
 * @author Graham Pople
 * @since 1.0.0
 */
public class Insert extends MutateInSpec {
    private final String path;
    private final Object doc;
    private boolean xattr = false;
    private boolean expandMacro = false;
    private boolean createPath = false;
    private JsonSerializer serializer;

    Insert(String path, Object doc) {
        this.path = path;
        if (doc instanceof MutateInMacro) {
            this.doc = ((MutateInMacro) doc).value();
            expandMacro = true;
            xattr = true;
        }
        else {
            this.doc = doc;
        }
    }

    /**
     * Sets that this is an extended attribute (xattr) field.
     * @return this, for chaining
     */
    public Insert xattr() {
        xattr = true;
        return this;
    }

    /**
     * Sets that this parent fields should be created automatically.
     * @return this, for chaining
     */
    public Insert createPath() {
        createPath = true;
        return this;
    }

    /**
     * Allows to customize the serializer used to encode the value.
     *
     * @param serializer the serializer that should be used.
     * @return this, for chaining
     */
    @Deprecated
    @Stability.Internal
    public Insert serializer(final JsonSerializer serializer) {
        notNull(serializer, "Serializer");
        this.serializer = serializer;
        return this;
    }

    public SubdocMutateRequest.Command encode(final JsonSerializer defaultSerializer, int originalIndex) {
        JsonSerializer serializer = this.serializer == null ? defaultSerializer : this.serializer;

        return new SubdocMutateRequest.Command(
            SubdocCommandType.DICT_ADD,
            path,
            serializer.serialize(doc),
            createPath,
            xattr,
            expandMacro,
            originalIndex
        );
    }
}
