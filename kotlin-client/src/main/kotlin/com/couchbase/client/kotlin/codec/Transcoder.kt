/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin.codec

import com.couchbase.client.kotlin.CommonOptions

public interface Transcoder {

    public fun <T> encode(input: T, type: TypeRef<T>): Content {
        return when (input) {
            is Content -> input // already encoded!
            is CommonOptions -> throw IllegalArgumentException(
                "Expected document content but got ${CommonOptions::class.java.simpleName}." +
                        " Was the 'content' argument accidentally omitted?"
            )
            else -> doEncode(input, type)
        }
    }

    /**
     * Encodes the given input into the wire representation based on the data format.
     *
     * @param input the object to encode.
     * @param type reified type of the object to encode.
     * @return the encoded wire representation of the payload.
     */
    public fun <T> doEncode(input: T, type: TypeRef<T>): Content

    /**
     * Decodes the wire representation into the entity based on the data format.
     *
     * @param content the wire representation to decode.
     * @param type the desired result type.
     * @return the decoded entity.
     */
    public fun <T> decode(content: Content, type: TypeRef<T>): T
}

