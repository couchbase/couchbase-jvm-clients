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
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.java.codec.JsonSerializer;

import static com.couchbase.client.core.msg.kv.SubdocCommandType.REPLACE_BODY_WITH_XATTR;

@Stability.Internal
public class ReplaceBodyWithXattr extends MutateInSpec {
    private static final byte[] FRAGMENT = new byte[]{};
    private final String src;

    public ReplaceBodyWithXattr(String src) {
        this.src = src;
    }

    public SubdocMutateRequest.Command encode(final JsonSerializer serializer, int originalIndex) {
        return new SubdocMutateRequest.Command(
                REPLACE_BODY_WITH_XATTR,
                src,
                FRAGMENT,
                false,
                true,
                false,
                originalIndex
        );
    }
}
