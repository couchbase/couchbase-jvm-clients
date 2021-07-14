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

package com.couchbase.client.kotlin.kv

import com.couchbase.client.core.error.subdoc.PathExistsException
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.core.msg.kv.CodecFlags.JSON_COMPAT_FLAGS
import com.couchbase.client.core.msg.kv.SubdocCommandType
import com.couchbase.client.core.msg.kv.SubdocMutateRequest
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.codec.Content
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.TypeRef
import com.couchbase.client.kotlin.codec.typeRef
import java.io.ByteArrayOutputStream

public class SubdocLong internal constructor(
    public val path: String,
    public val xattr: Boolean,
    internal val spec: MutateInSpec,
    internal val index: Int,
) {
    public fun get(result: MutateInResult): Long = with(result) { value }

    override fun toString(): String {
        return "SubdocLong(path='$path', xattr=$xattr)"
    }
}

@OptIn(VolatileCouchbaseApi::class)
public class MutateInSpec {

    internal class Command<T>(
        private val type: SubdocCommandType,
        private val path: String,
        private val xattr: Boolean,
        private val fragment: T,
        private val fragmentType: TypeRef<T>,
        private val index: Int,
        private val createParent: Boolean?,
        private val arrayElementType: TypeRef<*>?,
    ) {
        @OptIn(VolatileCouchbaseApi::class)
        internal fun encode(defaultCreateParent: Boolean, serializer: JsonSerializer): SubdocMutateRequest.Command {
            if (fragment is MutateInMacro) {
                return SubdocMutateRequest.Command(
                    type,
                    path,
                    serializer.serialize(fragment.value, typeRef()),
                    createParent ?: defaultCreateParent,
                    true, // macro implies xattr
                    true,
                    index
                )
            }

            val fragmentBytes = when {
                fragment is Content -> {
                    require(fragment.flags == JSON_COMPAT_FLAGS) { "Content must be JSON. Actual flags: ${fragment.flags}" }
                    fragment.bytes
                }
                arrayElementType != null -> serializeArrayFragment(fragment as List<*>, arrayElementType, serializer)
                else -> serializer.serialize(fragment, fragmentType)
            }

            return SubdocMutateRequest.Command(
                type,
                path,
                fragmentBytes,
                createParent ?: defaultCreateParent,
                xattr,
                false,
                index
            )
        }
    }

    internal var executed = false
    internal val commands: MutableList<Command<*>> = ArrayList()

    internal fun checkNotExecuted(): Unit =
        check(!executed) { "This MutateInSpec has already been executed. Create a fresh one for each call to Collection.mutateIn()." }

    @PublishedApi
    internal fun <T> addCommand(
        type: SubdocCommandType,
        path: String,
        xattr: Boolean,
        fragment: T,
        fragmentType: TypeRef<T>,
        createParent: Boolean? = null,
        arrayElementType: TypeRef<*>? = null,
    ) {
        checkNotExecuted()
        commands.add(Command(type, path, xattr, fragment, fragmentType, commands.size, createParent, arrayElementType))
    }

    /**
     * Inserts an Object node field. Fails if the field already exists.
     */
    public inline fun <reified T> insert(
        path: String,
        value: T,
        xattr: Boolean = false,
    ): Unit = addCommand(
        SubdocCommandType.DICT_ADD, path, xattr, value, typeRef(),
    )

    /**
     * Upserts an Object node field.
     */
    public inline fun <reified T> upsert(
        path: String,
        value: T,
        xattr: Boolean = false,
    ): Unit = addCommand(
        SubdocCommandType.DICT_UPSERT, path, xattr, value, typeRef(),
    )

    /**
     * Replaces an Object node field or Array element.
     */
    public inline fun <reified T> replace(
        path: String,
        value: T,
        xattr: Boolean = false,
    ): Unit {
        val type = if (path.isEmpty()) SubdocCommandType.SET_DOC else SubdocCommandType.REPLACE
        return addCommand(type, path, xattr, value, typeRef(), createParent = false)
    }

    /**
     * Removes the targeted element (or whole doc if path is empty)
     */
    public fun remove(
        path: String,
        xattr: Boolean = false,
    ): Unit {
        val type = if (path.isEmpty()) SubdocCommandType.DELETE_DOC else SubdocCommandType.DELETE
        addCommand(type, path, xattr, Content.json(byteArrayOf()), typeRef<Any?>(), createParent = false)
    }

    public inline fun <reified T> arrayAppend(
        path: String, values: List<T>,
        xattr: Boolean = false,
    ): Unit = addCommand(
        SubdocCommandType.ARRAY_PUSH_LAST, path, xattr, values, typeRef(), arrayElementType = typeRef<T>()
    )

    public inline fun <reified T> arrayPrepend(
        path: String, values: List<T>,
        xattr: Boolean = false,
    ): Unit = addCommand(
        SubdocCommandType.ARRAY_PUSH_FIRST, path, xattr, values, typeRef(), arrayElementType = typeRef<T>()
    )

    public inline fun <reified T> arrayInsert(
        path: String, values: List<T>,
        xattr: Boolean = false,
    ): Unit = addCommand(
        SubdocCommandType.ARRAY_INSERT,
        path,
        xattr,
        values,
        typeRef(),
        createParent = false,
        arrayElementType = typeRef<T>()
    )

    /**
     * Adds the value to an array (creating the array if it doesn't already exist)
     * or fail with [PathExistsException] if the array already contains the value.
     *
     * @param path Path to the array to modify or create
     * @param value Value to add to the array if not already present
     */
    public fun arrayAddUnique(
        path: String,
        value: Boolean?,
        xattr: Boolean = false,
    ): Unit = doArrayAddUnique(path, value, xattr)

    /**
     * Adds the value to an array (creating the array if it doesn't already exist)
     * or fail with [PathExistsException] if the array already contains the value.
     *
     * @param path Path to the array to modify or create
     * @param value Value to add to the array if not already present
     */
    public fun arrayAddUnique(
        path: String,
        value: String?,
        xattr: Boolean = false,
    ): Unit = doArrayAddUnique(path, value, xattr)

    /**
     * Adds the value to an array (creating the array if it doesn't already exist)
     * or fail with [PathExistsException] if the array already contains the value.
     *
     * @param path Path to the array to modify or create
     * @param value Value to add to the array if not already present
     */
    public fun arrayAddUnique(
        path: String,
        value: Long?,
        xattr: Boolean = false,
    ): Unit = doArrayAddUnique(path, value, xattr)

    /**
     * Adds the value to an array (creating the array if it doesn't already exist)
     * or fail with [PathExistsException] if the array already contains the value.
     *
     * @param path Path to the array to modify or create
     * @param value Value to add to the array if not already present
     */
    public fun arrayAddUnique(
        path: String,
        value: Int?,
        xattr: Boolean = false,
    ): Unit = doArrayAddUnique(path, value, xattr)

    private fun doArrayAddUnique(
        path: String,
        value: Any?,
        xattr: Boolean,
    ): Unit = addCommand(
        SubdocCommandType.ARRAY_ADD_UNIQUE,
        path,
        xattr,
        Content.json(Mapper.encodeAsBytes(value)),
        typeRef()
    )

    public fun incrementAndGet(
        path: String,
        delta: Long = 1,
        xattr: Boolean = false,
    ): SubdocLong {
        val subdoc = SubdocLong(path, xattr, this, commands.size)
        addCommand(SubdocCommandType.COUNTER, path, xattr, delta, typeRef())
        return subdoc
    }

    public fun decrementAndGet(
        path: String,
        delta: Long = 1,
        xattr: Boolean = false,
    ): SubdocLong = incrementAndGet(path, -delta, xattr)
}

@OptIn(VolatileCouchbaseApi::class)
private fun <T> serializeArrayFragment(
    values: List<*>,
    elementType: TypeRef<T>,
    serializer: JsonSerializer
): ByteArray = values
    .map {
        require(it !is MutateInMacro) { "MutateInMacro in subdoc array not supported" }

        @Suppress("UNCHECKED_CAST")
        serializer.serialize(it as T, elementType)
    }
    .join(delimiter = byteArrayOf(','.code.toByte()))

private fun List<ByteArray>.join(delimiter: ByteArray = byteArrayOf()): ByteArray {
    val resultSize = map { it.size }.sum() + delimiter.size * (size - 1)
    val result = ByteArrayOutputStream(resultSize)
    forEachIndexed { i, value ->
        result.write(value)
        if (i != size - 1) result.write(delimiter)
    }
    return result.toByteArray()
}
