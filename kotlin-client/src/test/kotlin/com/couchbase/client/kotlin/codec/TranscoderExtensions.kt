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


internal inline fun <reified T> Transcoder.decode(content: Content): T {
    return decode(content, typeRef())
}

internal inline fun <reified T> Transcoder.encode(input: T): Content {
    return encode(input, typeRef())
}

internal inline fun <reified T> JsonSerializer.serialize(input: T): ByteArray {
    return serialize(input, typeRef())
}

internal inline fun <reified T> JsonSerializer.deserialize(input: ByteArray): T {
    return deserialize(input, typeRef())
}
