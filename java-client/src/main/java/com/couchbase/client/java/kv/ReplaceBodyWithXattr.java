/*
 * Copyright (c) 2021 Couchbase, Inc.
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
import com.couchbase.client.core.api.kv.CoreSubdocMutateCommand;
import com.couchbase.client.core.util.Bytes;
import com.couchbase.client.java.codec.JsonSerializer;

import static com.couchbase.client.core.msg.kv.SubdocCommandType.REPLACE_BODY_WITH_XATTR;

@Stability.Internal
public class ReplaceBodyWithXattr extends MutateInSpec {
    private static final byte[] FRAGMENT = Bytes.EMPTY_BYTE_ARRAY;
    private final String src;

    public ReplaceBodyWithXattr(String src) {
        this.src = src;
    }

    @Override
    public CoreSubdocMutateCommand toCore(final JsonSerializer serializer) {
        return new CoreSubdocMutateCommand(
                REPLACE_BODY_WITH_XATTR,
                src,
                FRAGMENT,
                false,
                true,
                false
        );
    }
}
