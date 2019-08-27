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
package com.couchbase.client.java.datastructures;

import java.util.AbstractQueue;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Queue;

import com.couchbase.client.core.annotation.Stability;


import com.couchbase.client.core.error.KeyExistsException;
import com.couchbase.client.core.msg.kv.SubDocumentOpResponseStatus;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;
import com.couchbase.client.core.error.CASMismatchException;
import com.couchbase.client.core.error.subdoc.MultiMutationException;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInSpec;

/**
 * A CouchbaseQueue is a {@link Queue} backed by a {@link Collection Couchbase} document (more
 * specifically a {@link JsonArray JSON array}).
 *
 * Note that as such, a CouchbaseQueue is restricted to the types that a {@link JsonArray JSON array}
 * can contain. JSON objects and sub-arrays can be represented as {@link JsonObject} and {@link JsonArray}
 * respectively. Null values are not allowed as they have special meaning for the {@link #peek()} and {@link #remove()}
 * methods of a queue.
 *
 * @param <E> the type of values in the queue.
 *
 * @author Simon Baslé
 * @author Subhashni Balakrishnan
 * @since 2.3.6
 */

@Stability.Committed
public class CouchbaseQueue<E> extends AbstractQueue<E> {

    private static final int MAX_OPTIMISTIC_LOCKING_ATTEMPTS = Integer.parseInt(System.getProperty("com.couchbase.datastructureCASRetryLimit", "10"));
    private final String id;
    private final Collection collection;
    private final Class<E> entityTypeClass;
    /**
     * Create a new {@link Collection Couchbase-backed} Queue, backed by the document identified by <code>id</code>
     * in <code>bucket</code>. Note that if the document already exists, its content will be used as initial
     * content for this collection. Otherwise it is created empty.
     *
     * @param id the id of the Couchbase document to back the queue.
     * @param collection the {@link Collection} through which to interact with the document.
     * @param entityType a {@link Class<E>} describing the type of objects in this Set.
     */
    public CouchbaseQueue(String id, Collection collection, Class<E> entityType) {
        this.collection = collection;
        this.id = id;
        this.entityTypeClass = entityType;

        try {
            this.collection.insert(id, JsonArray.empty());
        } catch (KeyExistsException ex) {
            // Ignore concurrent creations, keep on moving.
        }
    }

    @Override
    public Iterator<E> iterator() {
        GetResult result = collection.get(id);
        return (Iterator<E>) result.contentAsArray().iterator();
    }

    @Override
    public int size() {
        LookupInResult result = collection.lookupIn(id, Collections.singletonList(LookupInSpec.count("")));
        return result.contentAs(0, Integer.class);
    }

    @Override
    public void clear() {

        collection.upsert(id, JsonArray.empty());
    }

    @Override
    public boolean offer(E e) {
        if (e == null) {
            throw new NullPointerException("Unsupported null value");
        }
        collection.mutateIn(id, Collections.singletonList(MutateInSpec.arrayPrepend("", e)));
        return true;
    }

    @Override
    public E poll() {
        String idx = "[-1]"; //FIFO queue as offer uses ARRAY_PREPEND
        for(int i = 0; i < MAX_OPTIMISTIC_LOCKING_ATTEMPTS; i++) {
            try {
                LookupInResult result = collection.lookupIn(id, Collections.singletonList(LookupInSpec.get(idx)));
                //
                E current = result.contentAs(0, entityTypeClass);
                long returnCas = result.cas();
                collection.mutateIn(id, Collections.singletonList(MutateInSpec.remove(idx)), MutateInOptions.mutateInOptions().cas(returnCas));
                return current;
            } catch (CASMismatchException ex) {
                //will have to retry get-and-remove
            } catch (MultiMutationException ex) {
                if (ex.firstFailureStatus() == SubDocumentOpResponseStatus.PATH_NOT_FOUND) {
                    // queue is empty
                    return null;
                }
                throw ex;
            }
        }
        throw new ConcurrentModificationException("Couldn't perform poll in less than " + MAX_OPTIMISTIC_LOCKING_ATTEMPTS + " iterations");
    }

    @Override
    public E peek() {
        try {
            LookupInResult result = collection.lookupIn(id, Collections.singletonList(LookupInSpec.get("[-1]")));
            return result.contentAs(0, entityTypeClass);
        } catch (MultiMutationException ex) {
            if (ex.firstFailureStatus() == SubDocumentOpResponseStatus.PATH_NOT_FOUND) {
                return null; //the queue is empty
            }
            throw ex;
        }
    }
}