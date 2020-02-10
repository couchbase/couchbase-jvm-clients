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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.java.codec.JsonSerializer;

import java.util.List;

/**
 * Defines specs to mutate parts in a JSON document.
 * <p>
 * Note that each spec only specifies a portion of a document. Whether the whole document is to be upserted or
 * inserted is controlled by {@link MutateInOptions#storeSemantics(StoreSemantics)} on the outer options block.
 * <p>
 * Some operations allow to specify an empty path ("") which means that the root document level is used (so it
 * will be applied to the full document). By nature it makes sense to only use such a command in isolation, but
 * it can be combined with xattr (extended attributes, a document metadata section) operations as well.
 */
public abstract class MutateInSpec {

    /**
     * Internal operation called from the encoding side that encodes the spec into its internal representation.
     *
     * @param serializer the serializer that should be used.
     * @param originalIndex the original index of the command.
     * @return the encoded command.
     */
    @Stability.Internal
    public abstract SubdocMutateRequest.Command encode(JsonSerializer serializer, int originalIndex);

    /**
     * Creates a spec with the intention of replacing an existing value in a JSON document.
     * <p>
     * If the path is empty (""), then the value will be used for the document's full body. Will
     * error if the last element of the path does not exist.
     *
     * @param path the path identifying where to replace the value.
     * @param value the value to replace with.
     * @return the created {@link MutateInSpec}.
     */
    public static Replace replace(final String path, final Object value) {
        return new Replace(path, value);
    }

    /**
     * Creates a command with the intention of inserting a new value in a JSON object.
     * <p>
     * Will error if the last element of the path already exists.
     *
     * @param path the path identifying where to insert the value.
     * @param value the value to insert
     * @return the created {@link MutateInSpec}.
     */
    public static Insert insert(final String path, final Object value) {
        return new Insert(path, value);
    }

    /**
     * Creates a command with the intention of removing an existing value in a JSON object.
     * <p>
     * Will error if the path does not exist.
     *
     * @param path the path identifying what to remove.
     * @return the created {@link MutateInSpec}.
     */
    public static Remove remove(final String path) {
        return new Remove(path);
    }

    /**
     * Creates a command with the intention of upserting a value in a JSON object.
     * <p>
     * That is, the value will be replaced if the path already exists, or inserted if not.
     *
     * @param path the path identifying where to upsert the value.
     * @param value the value to upsert.
     * @return the created {@link MutateInSpec}.
     */
    public static Upsert upsert(final String path, final Object value) {
        return new Upsert(path, value);
    }

    /**
     * Creates a command with the intention of appending a value to an existing JSON array.
     * <p>
     * Will error if the last element of the path does not exist or is not an array.
     *
     * @param path the path identifying an array to which to append the value.
     * @param values the value(s) to append.
     * @return the created {@link MutateInSpec}.
     */
    public static ArrayAppend arrayAppend(final String path, final List<?> values) {
        return new ArrayAppend(path, values);
    }

    /**
     * Creates a command with the intention of prepending a value to an existing JSON array.
     * <p>
     * Will error if the last element of the path does not exist or is not an array.
     *
     * @param path the path identifying an array to which to append the value.
     * @param values the value(s) to prepend.
     * @return the created {@link MutateInSpec}.
     */
    public static ArrayPrepend arrayPrepend(final String path, final List<?> values) {
        return new ArrayPrepend(path, values);
    }

    /**
     * Creates a command with the intention of inserting a value into an existing JSON array.
     * <p>
     * Will error if the last element of the path does not exist or is not an array.
     *
     * @param path the path identifying an array to which to append the value, and an index.  E.g. "foo.bar[3]"
     * @param values the value(s) to insert.
     * @return the created {@link MutateInSpec}.
     */
    public static ArrayInsert arrayInsert(final String path, final List<?> values) {
        return new ArrayInsert(path, values);
    }

    /**
     * Creates a command with the intent of inserting a value into an existing JSON array, but only if the value
     * is not already contained in the array (by way of string comparison).
     * <p>
     * Will error if the last element of the path does not exist or is not an array.
     *
     * @param path the path identifying an array to which to append the value, and an index.  E.g. "foo.bar[3]"
     * @param value the value to insert.
     * @return the created {@link MutateInSpec}.
     */
    public static ArrayAddUnique arrayAddUnique(final String path, final Object value) {
        return new ArrayAddUnique(path, value);
    }

    /**
     * Creates a command with the intent of incrementing a numerical field in a JSON object.
     *
     * If the field does not exist then it is created and takes the value of `delta`.
     *
     * @param path the path identifying a numerical field to adjust or create.
     * @param delta the value to increment the field by.
     * @return the created {@link MutateInSpec}.
     */
    public static Increment increment(final String path, final long delta) {
        return new Increment(path, delta);
    }

    /**
     * Creates a command with the intent of decrementing a numerical field in a JSON object.
     *
     * If the field does not exist then it is created and takes the value of `delta` * -1.
     *
     * @param path the path identifying a numerical field to adjust or create.
     * @param delta the value to increment the field by.
     * @return the created {@link MutateInSpec}.
     */
    public static Increment decrement(final String path, final long delta) {
        return new Increment(path,delta * -1);
    }
}

