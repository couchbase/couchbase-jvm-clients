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

package com.couchbase.client.java.kv;

import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;

/**
 * An intention to perform a SubDocument increment operation.
 *
 * @author Graham Pople
 * @since 1.0.0
 */
public class Increment extends MutateInSpec {
    private final String path;
    private final byte[] delta;
    private boolean xattr = false;
    private boolean createPath = false;

    Increment(String path, final byte[] delta) {
        this.path = path;
        this.delta = delta;
    }

    /**
     * Sets that this is an extended attribute (xattr) field.
     * @return this, for chaining
     */
    public Increment xattr() {
        xattr = true;
        return this;
    }

    /**
     * Sets that this parent fields should be created automatically.
     * @return this, for chaining
     */
    public Increment createPath() {
        createPath = true;
        return this;
    }

    public SubdocMutateRequest.Command encode() {
        return new SubdocMutateRequest.Command(
                SubdocCommandType.COUNTER,
                path,
                delta,
                createPath,
                xattr,
                false
        );
    }
}
