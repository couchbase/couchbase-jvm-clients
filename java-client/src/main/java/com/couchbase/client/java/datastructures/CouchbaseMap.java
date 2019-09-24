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

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.couchbase.client.core.annotation.Stability;

import com.couchbase.client.core.error.KeyNotFoundException;
import com.couchbase.client.core.msg.kv.SubDocumentOpResponseStatus;
import com.couchbase.client.core.retry.reactor.RetryExhaustedException;
import com.couchbase.client.java.Bucket;

import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.core.error.CASMismatchException;
import com.couchbase.client.core.error.KeyExistsException;
import com.couchbase.client.core.error.subdoc.MultiMutationException;
import com.couchbase.client.core.error.subdoc.PathNotFoundException;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.LookupInOptions;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.MapOptions;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.kv.UpsertOptions;

/**
 * A CouchbaseMap is a {@link Map} backed by a {@link Bucket Couchbase} document (more specifically a
 * {@link JsonObject JSON object}).
 *
 * Null keys are NOT permitted, and keys are restricted to {@link String}.
 *
 * Values in a CouchbaseMap are restricted to the types that a {@link JsonObject JSON objects}
 * can contain. JSON sub-objects and sub-arrays can be represented as {@link JsonObject} and {@link JsonArray}
 * respectively.
 *
 * @param <E> the type of values in the map (restricted to {@link JsonObject}.
 *
 * @since 2.3.6
 */

@Stability.Committed
public class CouchbaseMap<E> extends AbstractMap<String, E> {

    private final String id;
    private final Collection collection;
    private final Class<E> entityTypeClass;
    private final MapOptions.Built mapOptions;
    private final GetOptions getOptions;
    private final LookupInOptions lookupInOptions;
    private final InsertOptions insertOptions;
    private final UpsertOptions upsertOptions;
    /**
     * Create a new {@link CouchbaseMap}, backed by the document identified by <code>id</code>
     * in the given Couchbase <code>bucket</code>. Note that if the document already exists,
     * its content will be used as initial content for this collection. Otherwise it is created empty.
     *
     * @param id the id of the Couchbase document to back the map.
     * @param collection the {@link Collection} through which to interact with the document.
     * @param entityType a {@link Class<E>} describing the type of objects used as values in this Map.
     * @param options a {@link MapOptions} to use for all operations on this instance of the map.
     */
    public CouchbaseMap(String id, Collection collection, Class<E> entityType, MapOptions options) {
        this.id = id;
        this.collection = collection;
        this.entityTypeClass = entityType;

        MapOptions.Built optionsIn = options.build();
        MapOptions mapOpts = MapOptions.mapOptions();

        optionsIn.copyInto(mapOpts);
        this.mapOptions = mapOpts.build();
        this.getOptions = optionsIn.getOptions();
        this.lookupInOptions = optionsIn.lookupInOptions();
        this.upsertOptions = optionsIn.upsertOptions();
        this.insertOptions = optionsIn.insertOptions();
    }

    /**
     * Create a new {@link CouchbaseMap}, backed by the document identified by <code>id</code>
     * in the given Couchbase <code>bucket</code>. Note that if the document already exists,
     * its content will be used as initial content for this collection. Otherwise it is created empty.
     *
     * @param id the id of the Couchbase document to back the map.
     * @param collection the {@link Collection} through which to interact with the document.
     * @param entityType a {@link Class<E>} describing the type of objects used as values in this Map.
     */
    public CouchbaseMap(String id, Collection collection, Class<E> entityType) {
        this(id, collection, entityType, MapOptions.mapOptions());
    }

    @Override
    public E put(String key, E value) {
        checkKey(key);

        for(int i = 0; i < mapOptions.casMismatchRetries(); i++) {
            try {
                long returnCas = 0;
                E result = null;
                try {
                    LookupInResult current = collection.lookupIn(id,
                            Collections.singletonList(LookupInSpec.get(key)),
                            lookupInOptions);
                    returnCas = current.cas();
                    if (current.exists(0)) {
                        result = current.contentAs(0, entityTypeClass);
                    }
                } catch (PathNotFoundException e) {
                    // that's ok, we will just upsert anyways, and return null
                } catch (KeyNotFoundException e) {
                    // we will create an empty doc and remember the cas
                    returnCas = createEmpty();
                }
                collection.mutateIn(id,
                        Collections.singletonList(MutateInSpec.upsert(key, value)),
                        mapOptions.mutateInOptions().cas(returnCas));
                return result;
            } catch (CASMismatchException ex) {
                //will need to retry get-and-set
            }
        }
        throw new RetryExhaustedException("Couldn't perform set in less than " + mapOptions.casMismatchRetries() + " iterations.  It is likely concurrent modifications of this document are the reason");
    }

    @Override
    public E get(Object key) {
        String idx = checkKey(key);
        try {
            return collection.lookupIn(id,
                    Collections.singletonList(LookupInSpec.get(idx)),
                    lookupInOptions)
                    .contentAs(0, entityTypeClass);
        } catch (PathNotFoundException e) {
            return null;
        } catch (KeyNotFoundException e) {
            return null;
        }
    }



    @Override
    public E remove(Object key) {
        String idx = checkKey(key);
        for(int i = 0; i < mapOptions.casMismatchRetries(); i++) {
            try {
                LookupInResult current = collection.lookupIn(id,
                        Collections.singletonList(LookupInSpec.get(idx)),
                        lookupInOptions);
                long returnCas = current.cas();
                E result = current.contentAs(0, entityTypeClass);
                collection.mutateIn(id,
                        Collections.singletonList(MutateInSpec.remove(idx)),
                        mapOptions.mutateInOptions().cas(returnCas));
                return result;
            } catch (KeyNotFoundException e) {
                return null;
            } catch (CASMismatchException ex) {
                //will have to retry get-and-remove
            } catch (MultiMutationException ex) {
                if (ex.firstFailureStatus() == SubDocumentOpResponseStatus.PATH_NOT_FOUND) {
                    return null;
                }
                throw ex;
            }
        }
        throw new RetryExhaustedException("Couldn't perform set in less than " + mapOptions.casMismatchRetries() + " iterations.  It is likely concurrent modifications of this document are the reason");
    }

    @Override
    public void clear() {
        //optimized version over AbstractMap's (which uses the entry set)
        collection.remove(id);
    }

    @Override
    public Set<Entry<String, E>> entrySet() {
        JsonObject obj;
        try {
            obj = collection.get(id, getOptions).contentAsObject();
        } catch (KeyNotFoundException e) {
            obj = JsonObject.empty();
        }
        // don't actually create the doc, yet.
        return new CouchbaseEntrySet((Map<String, E> )obj.toMap());
    }

    @Override
    public boolean containsKey(Object key) {
        String idx = checkKey(key);
        try {
            return collection.lookupIn(id,
                    Collections.singletonList(LookupInSpec.exists(idx)),
                    lookupInOptions)
                    .exists(0);
        } catch (KeyNotFoundException e) {
            return false;
        }

    }

    @Override
    public boolean containsValue(Object value) {
        return super.containsValue(value); //TODO use ARRAY_CONTAINS subdoc operator when available
    }

    @Override
    public int size() {
        try {
            LookupInResult current = collection.lookupIn(id,
                    Collections.singletonList(LookupInSpec.count("")),
                    lookupInOptions);
            return current.contentAs(0, Integer.class);
        } catch (KeyNotFoundException e) {
            return 0;
        }
    }

    private String checkKey(Object key) {
        if (key == null) {
            throw new NullPointerException("Unsupported null key");
        }
        return String.valueOf(key);
    }

    private class CouchbaseEntrySet implements Set<Map.Entry<String, E>> {

        private final Set<Map.Entry<String, E>> delegate;

        private CouchbaseEntrySet(Map<String, E> data) {
            this.delegate = data.entrySet();
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public boolean isEmpty() {
            return delegate.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return delegate.contains(o);
        }

        @Override
        public Iterator<Entry<String, E>> iterator() {
            return new CouchbaseEntrySetIterator(delegate.iterator());
        }

        @Override
        public Object[] toArray() {
            return delegate.toArray();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return delegate.toArray(a);
        }

        @Override
        public boolean add(Entry<String, E> stringVEntry) {
            return delegate.add(stringVEntry);
        }

        @Override
        public boolean remove(Object o) {
            if (delegate.remove(o)) {
                if (o instanceof Map.Entry) {
                    Entry<String, E> entry = (Entry<String, E>) o;
                    CouchbaseMap.this.remove(entry.getKey());
                } else {
                    throw new IllegalStateException("Expected entrySet remove() to remove an entry");
                }
                return true;
            } else {
                return false;
            }
        }

        @Override
        public boolean containsAll(java.util.Collection<?> c) {
            return delegate.containsAll(c);
        }

        @Override
        public boolean addAll(java.util.Collection<? extends Entry<String, E>> c) {
            return delegate.addAll(c);
        }

        @Override
        public boolean retainAll(java.util.Collection<?> c) {
            return delegate.retainAll(c);
        }

        @Override
        public boolean removeAll(java.util.Collection<?> c) {
            return delegate.removeAll(c);
        }

        @Override
        public void clear() {
            delegate.clear();
            CouchbaseMap.this.clear();
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

        public int hashCode() {
            return delegate.hashCode();
        }

        public boolean equals(Object obj) {
            return delegate.equals(obj);
        }
    }

    private class CouchbaseEntrySetIterator implements Iterator<Entry<String, E>> {

        private final Iterator<Entry<String, E>> delegateItr;
        private Entry<String, E> lastNext = null;

        public CouchbaseEntrySetIterator(Iterator<Entry<String, E>> iterator) {
            this.delegateItr = iterator;
        }

        @Override
        public boolean hasNext() {
            return delegateItr.hasNext();
        }

        @Override
        public Entry<String, E> next() {
            this.lastNext = delegateItr.next();
            return lastNext;
        }

        @Override
        public void remove() {
            if (lastNext == null)
                throw new IllegalStateException("next() hasn't been called before remove()");
            delegateItr.remove();
            CouchbaseMap.this.remove(lastNext.getKey());
        }
    }

    protected long createEmpty() {
        try {
            return collection.insert(id, JsonObject.empty(), insertOptions).cas();
        } catch (KeyExistsException ex) {
            // Ignore concurrent creations, keep on moving.
            // but we need the cas, so...
            return collection.get(id, getOptions).cas();
        }
    }
}