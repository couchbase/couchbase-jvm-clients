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
import com.couchbase.client.core.util.Bytes;
import com.couchbase.client.java.codec.JsonSerializer;

/**
 * An intention to perform a SubDocument remove operation.
 *
 * @author Graham Pople
 * @since 1.0.0
 */
public class Remove extends MutateInSpec {
    private final String path;
    private boolean xattr = false;

    Remove(String path) {
        this.path = path;
    }

    /**
     * Sets that this is an extended attribute (xattr) field.
     * @return this, for chaining
     */
    public Remove xattr() {
        xattr = true;
        return this;
    }

    @Override
    public CoreSubdocMutateCommand toCore(JsonSerializer serializer) {
        return new CoreSubdocMutateCommand(
                path.isEmpty() ? SubdocCommandType.DELETE_DOC : SubdocCommandType.DELETE,
                path,
                Bytes.EMPTY_BYTE_ARRAY,
                false,
                xattr,
                false
        );
    }
}
