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
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Set;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.KeyExistsException;
import com.couchbase.client.core.error.subdoc.MultiMutationException;
import com.couchbase.client.core.msg.kv.SubDocumentOpResponseStatus;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;
import com.couchbase.client.core.error.CASMismatchException;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInResult;
import com.couchbase.client.java.kv.MutateInSpec;

/**
 * A CouchbaseArraySet is a {@link Set} backed by a {@link Collection Couchbase} document (more
 * specifically a {@link JsonArray JSON array}).
 *
 * Note that a CouchbaseArraySet is restricted to primitive types (the types that a {@link JsonArray JSON array}
 * can contain, except {@link JsonObject} and {@link JsonArray}). null entries are supported.
 *
 * @param <T> the type of values in the set.
 *
 * @since 2.3.6
 */

@Stability.Committed
public class CouchbaseArraySet<T> extends AbstractSet<T> {

    private static final int MAX_OPTIMISTIC_LOCKING_ATTEMPTS = Integer.parseInt(System.getProperty("com.couchbase.datastructureCASRetryLimit", "10"));
    private final String id;
    private final Collection collection;
    private final Class<T> entityTypeClass;

    /**
     * Create a new {@link CouchbaseArraySet}, backed by the document identified by <code>id</code>
     * in the given Couchbase <code>bucket</code>. Note that if the document already exists,
     * its content will be used as initial content for this collection. Otherwise it is created empty.
     *
     * @param id the id of the Couchbase document to back the set.
     * @param collection the {@link Collection} through which to interact with the document.
     * @param entityType a Class<T> describing the type of objects in this Set.
     *
     **/
    public CouchbaseArraySet(String id, Collection collection, Class<T> entityType) {
        this.id = id;
        this.collection = collection;
        this.entityTypeClass = entityType;

        try {
            this.collection.insert(id, JsonArray.empty());
        } catch (KeyExistsException e) {
            //use a pre-existing document
        }
    }

    @Override
    public int size() {
        LookupInResult result = collection.lookupIn(id, Collections.singletonList(LookupInSpec.count("")));
        return result.contentAs(0, Integer.class);
    }

    @Override
    public boolean isEmpty() {
        LookupInResult current = collection.lookupIn(id, Collections.singletonList(LookupInSpec.exists("[0]")));
        return !current.exists(0);
    }

    @Override
    public boolean contains(Object t) {
        //TODO subpar implementation for a Set, use ARRAY_CONTAINS when available
        enforcePrimitive(t);
        GetResult result = collection.get(id);
        JsonArray current = result.contentAs(JsonArray.class);
        for (Object in : current) {
            if (safeEquals(in, t)) {
                return true;
            }
        }
        return false;
    }
    private class CouchbaseArraySetIterator<E> implements Iterator<E> {
        private long cas;
        private final Iterator<E> delegate;
        private int lastVisited;
        private int cursor;

        public CouchbaseArraySetIterator() {
            GetResult result = collection.get(id);
            JsonArray current = result.contentAs(JsonArray.class);
            // We use a list rather than a set, so the index of the
            // removed item matches the index in the actual document in
            // the server
            ArrayList<E> list = new ArrayList<E>(current.size());
            for (E value : (Iterable<E>) current) {
                list.add(value);
            }

            this.cas = result.cas();
            this.delegate = list.iterator();
            this.lastVisited = -1;
            this.cursor = 0;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public E next() {
            E next = delegate.next();
            lastVisited = cursor;
            cursor++;
            return next;
        }

        @Override
        public void remove() {
            if (lastVisited < 0) {
                throw new IllegalStateException();
            }
            // we simply want to remove lastVisited from
            // the document on the server, and this set, assuming
            // nothing has changed.
            int index = lastVisited;
            String idx = "[" + index + "]";
            try {
                MutateInResult updated = collection.mutateIn(
                        id,
                        Collections.singletonList(MutateInSpec.remove(idx)),
                        MutateInOptions.mutateInOptions().cas(this.cas)
                );
                //update the cas so that several removes in a row can work
                this.cas = updated.cas();
                //also correctly reset the state:
                delegate.remove();
                this.cursor = lastVisited;
                this.lastVisited = -1;
            } catch (CASMismatchException ex) {
                throw new ConcurrentModificationException("List was modified since iterator creation: " + ex);
            } catch (MultiMutationException ex) {
                if (ex.firstFailureStatus() == SubDocumentOpResponseStatus.PATH_NOT_FOUND) {
                    throw new ConcurrentModificationException("Element doesn't exist anymore at index: " + index);
                }
                throw ex;
            }
        }

    }

    @Override
    public Iterator<T> iterator() {
        return new CouchbaseArraySetIterator<>();
    }

    @Override
    public boolean add(T t) {
        enforcePrimitive(t);

        try {
            collection.mutateIn(id, Collections.singletonList(MutateInSpec.arrayAddUnique("", t)));
            return true;
        } catch (MultiMutationException ex) {
            if (ex.firstFailureStatus() == SubDocumentOpResponseStatus.PATH_EXISTS) {
                return false;
            }
            throw ex;
        }
    }

    @Override
    public boolean remove(Object t) {
        enforcePrimitive(t);

        for (int i = 0; i < MAX_OPTIMISTIC_LOCKING_ATTEMPTS; i++) {
            try {
                GetResult result = collection.get(id);
                JsonArray current = result.contentAsArray();
                long cas = result.cas();
                int index = 0;
                boolean found = false;
                Iterator<Object> it = current.iterator();
                while(it.hasNext()) {
                    Object next = it.next();
                    if (safeEquals(next, t)) {
                        found = true;
                        break;
                    }
                    index++;
                }
                String path = "[" + index + "]";

                if (!found) {
                    return false;
                } else {
                    collection.mutateIn(id,
                            Collections.singletonList(MutateInSpec.remove(path)),
                            MutateInOptions.mutateInOptions().cas(cas));
                    return true;
                }
            } catch (CASMismatchException e) {
                //retry
            }
        }
        throw new ConcurrentModificationException("Couldn't perform remove in less than " + MAX_OPTIMISTIC_LOCKING_ATTEMPTS + " iterations");
    }

    @Override
    public void clear() {
        collection.upsert(id, JsonArray.empty());
    }

    /**
     * Verify that the type of object t is compatible with CouchbaseArraySet storage.
     *
     * @param t the object to check.
     * @throws ClassCastException if the object is incompatible.
     */
    protected void enforcePrimitive(Object t) throws ClassCastException {
        if (!JsonValue.checkType(t)
                || t instanceof JsonValue) {
            throw new ClassCastException("Only primitive types are supported in CouchbaseArraySet, got a " + t.getClass().getName());
        }
    }

    protected boolean safeEquals(Object expected, Object tested) {
        if (expected == null) {
            return tested == null;
        }
        return expected.equals(tested);
    }
}