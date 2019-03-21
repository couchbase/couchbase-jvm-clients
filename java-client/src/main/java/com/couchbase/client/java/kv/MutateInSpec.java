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
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.java.codec.DefaultEncoder;
import com.couchbase.client.java.codec.Encoder;

public abstract class MutateInSpec {
    @Stability.Internal
    public abstract SubdocMutateRequest.Command encode();

    private static final Encoder ENCODER = new DefaultEncoder();

    /**
     * Creates a command with the intention of replacing an existing value in a JSON object.
     * <p>
     * Will error if the last element of the path does not exist.
     *
     * @param path     the path identifying where to replace the value.
     * @param fragment the value to replace with
     */
    public static <T> Replace replace(final String path, final T fragment) {
        return replace(path, fragment, ENCODER);
    }

    /**
     * Creates a command with the intention of replacing an existing value in a JSON object.
     * <p>
     * Will error if the last element of the path does not exist.
     *
     * @param path     the path identifying where to replace the value.
     * @param fragment the value to replace with
     * @param encoder  a custom Encoder
     */
    public static <T> Replace replace(final String path, final T fragment, final Encoder encoder) {
        EncodedDocument doc = encoder.encode(fragment);
        return new Replace(path, doc);
    }

    /**
     * Creates a command with the intention of inserting a new value in a JSON object.
     * <p>
     * Will error if the last element of the path already exists.
     *
     * @param path     the path identifying where to insert the value.
     * @param fragment the value to insert
     */
    public static <T> Insert insert(final String path, final T fragment) {
        return insert(path, fragment, ENCODER);
    }

    /**
     * Creates a command with the intention of inserting a new value in a JSON object.
     * <p>
     * Will error if the last element of the path already exists.
     *
     * @param path     the path identifying where to insert the value.
     * @param fragment the value to insert
     * @param encoder  a custom Encoder
     */
    public static <T> Insert insert(final String path, final T fragment, final Encoder encoder) {
        EncodedDocument doc = encoder.encode(fragment);
        return new Insert(path, doc);
    }

    /**
     * Creates a command with the intention of removing an existing value in a JSON object.
     * <p>
     * Will error if the path does not exist.
     *
     * @param path the path identifying what to remove
     */
    public static <T> Remove remove(final String path) {
        return new Remove(path);
    }

    /**
     * Creates a command with the intention of upserting a value in a JSON object.
     * <p>
     * That is, the value will be replaced if the path already exists, or inserted if not.
     *
     * @param path     the path identifying where to upsert the value.
     * @param fragment the value to upsert
     */
    public static <T> Upsert upsert(final String path, final T fragment) {
        return upsert(path, fragment, ENCODER);
    }

    /**
     * Creates a command with the intention of upserting a value in a JSON object.
     * <p>
     * That is, the value will be replaced if the path already exists, or inserted if not.
     *
     * @param path     the path identifying where to upsert the value.
     * @param fragment the value to upsert
     * @param encoder  a custom Encoder
     */
    public static <T> Upsert upsert(final String path, final T fragment, final Encoder encoder) {
        EncodedDocument doc = encoder.encode(fragment);
        return new Upsert(path, doc);
    }

    /**
     * Creates a command with the intention of upserting the full body of a JSON document.
     * <p>
     * Provided to support advanced workflows that need to set a document's extended attributes (xattrs)
     * at the same time as the document's regular content.
     *
     * @param fragment the value to upsert to the document's body
     */
    public static <T> MutateInSpec fullDocument(final T fragment) {
        return fullDocument(fragment, ENCODER);
    }

    /**
     * Creates a command with the intention of upserting the full body of a JSON document.
     * <p>
     * Provided to support advanced workflows that need to set a document's extended attributes (xattrs)
     * at the same time as the document's regular content.
     *
     * @param fragment the value to upsert to the document's body
     * @param encoder  a custom Encoder
     */
    public static <T> MutateInSpec fullDocument(final T fragment, final Encoder encoder) {
        EncodedDocument doc = encoder.encode(fragment);
        return new FullDocument(doc);
    }

    /**
     * Creates a command with the intention of appending a value to an existing JSON array.
     * <p>
     * Will error if the last element of the path does not exist or is not an array.
     *
     * @param path     the path identifying an array to which to append the value.
     * @param fragment the value to append
     */
    public static <T> ArrayAppend arrayAppend(final String path, final T fragment) {
        return arrayAppend(path, fragment, ENCODER);
    }

    /**
     * Creates a command with the intention of appending a value to an existing JSON array.
     * <p>
     * Will error if the last element of the path does not exist or is not an array.
     *
     * @param path     the path identifying an array to which to append the value.
     * @param fragment the value to append
     * @param encoder  a custom Encoder
     */
    public static <T> ArrayAppend arrayAppend(final String path, final T fragment, final Encoder encoder) {
        EncodedDocument doc = encoder.encode(fragment);
        return new ArrayAppend(path, doc);
    }

    /**
     * Creates a command with the intention of prepending a value to an existing JSON array.
     * <p>
     * Will error if the last element of the path does not exist or is not an array.
     *
     * @param path     the path identifying an array to which to append the value.
     * @param fragment the value to prepend
     */
    public static <T> ArrayPrepend arrayPrepend(final String path, final T fragment) {
        return arrayPrepend(path, fragment, ENCODER);
    }

    /**
     * Creates a command with the intention of prepending a value to an existing JSON array.
     * <p>
     * Will error if the last element of the path does not exist or is not an array.
     *
     * @param path     the path identifying an array to which to append the value.
     * @param fragment the value to prepend
     * @param encoder  a custom Encoder
     */
    public static <T> ArrayPrepend arrayPrepend(final String path, final T fragment, final Encoder encoder) {
        EncodedDocument doc = encoder.encode(fragment);
        return new ArrayPrepend(path, doc);
    }

    /**
     * Creates a command with the intention of inserting a value into an existing JSON array.
     * <p>
     * Will error if the last element of the path does not exist or is not an array.
     *
     * @param path     the path identifying an array to which to append the value, and an index.  E.g. "foo.bar[3]"
     * @param fragment the value to insert
     */
    public static <T> ArrayInsert arrayInsert(final String path, final T fragment) {
        return arrayInsert(path, fragment, ENCODER);
    }

    /**
     * Creates a command with the intention of inserting a value into an existing JSON array.
     * <p>
     * Will error if the last element of the path does not exist or is not an array.
     *
     * @param path     the path identifying an array to which to append the value, and an index.  E.g. "foo.bar[3]"
     * @param fragment the value to insert
     * @param encoder  a custom Encoder
     */
    public static <T> ArrayInsert arrayInsert(final String path, final T fragment, final Encoder encoder) {
        EncodedDocument doc = encoder.encode(fragment);
        return new ArrayInsert(path, doc);
    }

    /**
     * Creates a command with the intent of inserting a value into an existing JSON array, but only if the value
     * is not already contained in the array (by way of string comparison).
     * <p>
     * Will error if the last element of the path does not exist or is not an array.
     *
     * @param path     the path identifying an array to which to append the value, and an index.  E.g. "foo.bar[3]"
     * @param fragment the value to insert
     */
    public static <T> ArrayAddUnique arrayAddUnique(final String path, final T fragment) {
        return arrayAddUnique(path, fragment, ENCODER);
    }

    /**
     * Creates a command with the intent of inserting a value into an existing JSON array, but only if the value
     * is not already contained in the array (by way of string comparison).
     * <p>
     * Will error if the last element of the path does not exist or is not an array.
     *
     * @param path     the path identifying an array to which to append the value, and an index.  E.g. "foo.bar[3]"
     * @param fragment the value to insert
     * @param encoder  a custom Encoder
     */
    public static <T> ArrayAddUnique arrayAddUnique(final String path, final T fragment, final Encoder encoder) {
        EncodedDocument doc = encoder.encode(fragment);
        return new ArrayAddUnique(path, doc);
    }

    /**
     * Creates a command with the intent of incrementing a numerical field in a JSON object.
     *
     * If the field does not exist then it is created and takes the value of `delta`.
     *
     * @param path       the path identifying a numerical field to adjust or create.
     * @param delta      the value to increment the field by.
     */
    public static <T> Increment increment(final String path, final long delta) {
        EncodedDocument doc = ENCODER.encode(delta);
        return new Increment(path, doc.content());
    }

    /**
     * Creates a command with the intent of decrementing a numerical field in a JSON object.
     *
     * If the field does not exist then it is created and takes the value of `delta` * -1.
     *
     * @param path       the path identifying a numerical field to adjust or create.
     * @param delta      the value to increment the field by.
     */
    public static <T> Increment decrement(final String path, final long delta) {
        EncodedDocument doc = ENCODER.encode(delta * -1);
        return new Increment(path, doc.content());
    }
}

