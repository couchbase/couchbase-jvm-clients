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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.subdoc.PathNotFoundException;
import com.couchbase.client.core.msg.kv.SubDocumentOpResponseStatus;
import com.couchbase.client.core.retry.reactor.RetryExhaustedException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.core.error.CASMismatchException;
import com.couchbase.client.core.error.KeyExistsException;
import com.couchbase.client.core.error.subdoc.MultiMutationException;
import com.couchbase.client.java.kv.ArrayListOptions;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.LookupInOptions;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInResult;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.kv.UpsertOptions;

/**
 * A CouchbaseArrayList is a {@link List} backed by a {@link Collection Couchbase} document (more
 * specifically a {@link JsonArray JSON array}).
 *
 * Note that as such, a CouchbaseArrayList is restricted to the types that a {@link JsonArray JSON array}
 * can contain. JSON objects and sub-arrays can be represented as {@link JsonObject} and {@link JsonArray}
 * respectively.
 *
 * @param <E> the type of values in the list.
 *
 * @since 2.3.6
 */
@Stability.Volatile
public class CouchbaseArrayList<E> extends AbstractList<E> {

    private final String id;
    private final Collection collection;
    private final ArrayListOptions.Built arrayListOptions;
    private final GetOptions getOptions;
    private final LookupInOptions lookupInOptions;
    private final MutateInOptions mutateInOptions;
    private final InsertOptions insertOptions;
    private final UpsertOptions upsertOptions;

    // TODO: perhaps there is a way around type erasure, so that we can eliminate this since it feels redundant?
    private Class<E> entityTypeClass;


    /**
     * Create a new {@link Collection Couchbase-backed} List, backed by the document identified by <code>id</code>
     * in <code>collection</code>. Note that if the document already exists, its content will be used as initial
     * content for this collection. Otherwise it is created empty.
     *
     * @param id the id of the Couchbase document to back the list.
     * @param collection the {@link Collection} through which to interact with the document.
     * @param entityType a Class<T> describing the type of objects in this Set.
     */
    public CouchbaseArrayList(String id, Collection collection, Class<E> entityType) {
        this(id, collection, entityType, ArrayListOptions.arrayListOptions());
    }
    /**
     * Create a new {@link Collection Couchbase-backed} List, backed by the document identified by <code>id</code>
     * in <code>collection</code>. Note that if the document already exists, its content will be used as initial
     * content for this collection. Otherwise it is created empty.
     *
     * @param id the id of the Couchbase document to back the list.
     * @param collection the {@link Collection} through which to interact with the document.
     * @param entityType a Class<T> describing the type of objects in this Set.
     * @param options a {@link ArrayListOptions} to use for all operations on this instance of the list.
     */
    public CouchbaseArrayList(String id, Collection collection, Class<E> entityType, ArrayListOptions options) {
        this.collection = collection;
        this.id = id;
        this.entityTypeClass = entityType;

        // copy the options just in case they are reused later somewhere else
        ArrayListOptions.Built optionsIn = options.build();
        ArrayListOptions opts = ArrayListOptions.arrayListOptions();
        optionsIn.copyInto(opts);
        this.arrayListOptions = opts.build();
        this.getOptions = optionsIn.getOptions();
        this.lookupInOptions = optionsIn.lookupInOptions();
        this.upsertOptions = optionsIn.upsertOptions();
        this.insertOptions = optionsIn.insertOptions();
        this.mutateInOptions = optionsIn.mutateInOptions();

        try {
            collection.insert(id, JsonArray.empty(), insertOptions);
        } catch (KeyExistsException ex) {
            // Ignore concurrent creations, keep on moving.
        }
    }

    @Override
    public E get(int index) {
        //fail fast on negative values, as they are interpreted as "starting from the back of the array" otherwise
        if (index < 0) {
            throw new IndexOutOfBoundsException("Index: " + index);
        }
        String idx = "[" + index + "]";
        LookupInResult result = collection.lookupIn(id,
                Collections.singletonList(LookupInSpec.get(idx)),
                lookupInOptions);
        if (!result.exists(0)) {
            throw new IndexOutOfBoundsException("Index: " + index);
        }

        return result.contentAs(0, entityTypeClass);
    }

    @Override
    public int size() {
        LookupInResult result = collection.lookupIn(id,
                Collections.singletonList(LookupInSpec.count("")),
                lookupInOptions);
        return result.contentAs(0, Integer.class);
    }

    @Override
    public boolean isEmpty() {
        LookupInResult current = collection.lookupIn(id,
                Collections.singletonList(LookupInSpec.exists("[0]")),
                lookupInOptions);
        return !current.exists(0);
    }

    @Override
    public E set(int index, E element) {
        //fail fast on negative values, as they are interpreted as "starting from the back of the array" otherwise
        if (index < 0) {
            throw new IndexOutOfBoundsException("Index: " + index);
        }
        String idx = "[" + index + "]";

        for(int i = 0; i < arrayListOptions.casMismatchRetries(); i++) {
            try {
                LookupInResult current = collection.lookupIn(id,
                        Collections.singletonList(LookupInSpec.get(idx)),
                        lookupInOptions);
                long returnCas = current.cas();
                // this loop ensures we return exactly what we replaced
                E result = current.contentAs(0, entityTypeClass);

                collection.mutateIn(id,
                        Collections.singletonList(MutateInSpec.replace(idx, element)),
                        arrayListOptions.mutateInOptions().cas(returnCas));
                return result;
            } catch (CASMismatchException ex) {
                //will need to retry get-and-set
            } catch (MultiMutationException ex) {
                if (ex.firstFailureStatus() == SubDocumentOpResponseStatus.PATH_NOT_FOUND) {
                    throw new IndexOutOfBoundsException("Index: " + index);
                }
                throw ex;
            }
        }
        throw new RetryExhaustedException("Couldn't perform set in less than " +  arrayListOptions.casMismatchRetries() + " iterations.  It is likely concurrent modifications of this document are the reason");
    }

    @Override
    public void add(int index, E element) {
        //fail fast on negative values, as they are interpreted as "starting from the back of the array" otherwise
        if (index < 0) {
            throw new IndexOutOfBoundsException("Index: " + index);
        }

        try {
            collection.mutateIn(id,
                    Collections.singletonList(MutateInSpec.arrayInsert("[" + index + "]", element)),
                    mutateInOptions);
        } catch (PathNotFoundException e) {
            throw new IndexOutOfBoundsException("Index: " + index);
        } catch (MultiMutationException ex) {
            if (ex.firstFailureStatus() == SubDocumentOpResponseStatus.PATH_NOT_FOUND) {
                throw new IndexOutOfBoundsException("Index: " + index);
            }
            throw ex;
        }
    }

    @Override
    public E remove(int index) {
        //fail fast on negative values, as they are interpreted as "starting from the back of the array" otherwise
        if (index < 0) {
            throw new IndexOutOfBoundsException("Index: " + index);
        }
        String idx = "[" + index + "]";
        for(int i = 0; i < arrayListOptions.casMismatchRetries(); i++) {
            try {
                // this loop will allow us to _know_ what element we really did remove.
                LookupInResult current = collection.lookupIn(id,
                        Collections.singletonList(LookupInSpec.get(idx)),
                        lookupInOptions);
                long returnCas = current.cas();
                E result = current.contentAs(0, entityTypeClass);
                MutateInResult updated = collection.mutateIn(id,
                        Collections.singletonList(MutateInSpec.remove(idx)),
                        arrayListOptions.mutateInOptions().cas(returnCas));
                return result;
            } catch (CASMismatchException ex) {
                //will have to retry get-and-remove
            } catch (PathNotFoundException e) {
                throw new IndexOutOfBoundsException("Index: " + index);
            } catch (MultiMutationException ex) {
                if (ex.firstFailureStatus() == SubDocumentOpResponseStatus.PATH_NOT_FOUND) {
                    throw new IndexOutOfBoundsException("Index: " + index);
                }
                throw ex;
            }
        }
        throw new RetryExhaustedException("Couldn't perform set in less than " + arrayListOptions.casMismatchRetries() + " iterations.  It is likely concurrent modifications of this document are the reason");
    }

    @Override
    public boolean contains(Object o) {
        // This grabs entire list locally, to search for o
        return super.contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        // This grabs entire list to create iterator
        return new CouchbaseListIterator(0);
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        // This grabs entire list to create iterator
        return new CouchbaseListIterator(index);
    }

    @Override
    public void clear() {
        //optimized version over AbstractList's (which iterates on all and remove)
       collection.upsert(id, JsonArray.empty(), upsertOptions);
    }

    private class CouchbaseListIterator implements ListIterator<E> {

        private long cas;
        private final ListIterator<E> delegate;

        private int cursor;
        private int lastVisited;

        public CouchbaseListIterator(int index) {
            GetResult result = collection.get(id, getOptions);
            JsonArray current = result.contentAs(JsonArray.class);
            //Care not to use toList, as it will convert internal JsonObject/JsonArray to Map/List
            List<E> list = new ArrayList<E>(current.size());
            for (E value : (Iterable<E>) current) {
                list.add(value);
            }

            this.cas = result.cas();
            this.delegate = list.listIterator(index);
            this.lastVisited = -1;
            this.cursor = index;
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
        public boolean hasPrevious() {
            return delegate.hasPrevious();
        }

        @Override
        public E previous() {
            E previous = delegate.previous();
            cursor--;
            lastVisited = cursor;
            return previous;
        }

        @Override
        public int nextIndex() {
            return delegate.nextIndex();
        }

        @Override
        public int previousIndex() {
            return delegate.previousIndex();
        }

        @Override
        public void remove() {
            if (lastVisited < 0) {
                throw new IllegalStateException();
            }
            int index = lastVisited;
            String idx = "[" + index + "]";
            try {
                MutateInResult updated = collection.mutateIn(
                        id,
                        Collections.singletonList(MutateInSpec.remove(idx)),
                        arrayListOptions.mutateInOptions().cas(cas));
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

        @Override
        public void set(E e) {
            if (lastVisited < 0) {
                throw new IllegalStateException();
            }
            int index = lastVisited;
            String idx = "[" + index + "]";
            try {
                MutateInResult updated = collection.mutateIn(
                        id,
                        Collections.singletonList(MutateInSpec.replace(idx, e)),
                        arrayListOptions.mutateInOptions().cas(cas));
                //update the cas so that several mutations in a row can work
                this.cas = updated.cas();
                //also correctly reset the state:
                delegate.set(e);
            } catch (CASMismatchException ex) {
                throw new ConcurrentModificationException("List was modified since iterator creation: " + ex);
            } catch (MultiMutationException ex) {
                if (ex.firstFailureStatus() == SubDocumentOpResponseStatus.PATH_NOT_FOUND) {
                    throw new ConcurrentModificationException("Element doesn't exist anymore at index: " + index);
                }
                throw ex;
            }
        }

        @Override
        public void add(E e) {
            int index = this.cursor;
            String idx = "[" + index + "]";
            try {
                MutateInResult updated = collection.mutateIn(
                        id,
                        Collections.singletonList(MutateInSpec.arrayInsert(idx, e)),
                        arrayListOptions.mutateInOptions().cas(cas));
                //update the cas so that several mutations in a row can work
                this.cas = updated.cas();
                //also correctly reset the state:
                delegate.add(e);
                this.cursor++;
                this.lastVisited = -1;
            } catch (CASMismatchException ex) {
                throw new ConcurrentModificationException("List was modified since iterator creation", ex);
            } catch (MultiMutationException ex) {
                if (ex.firstFailureStatus() == SubDocumentOpResponseStatus.PATH_NOT_FOUND) {
                    throw new ConcurrentModificationException("Element doesn't exist anymore at index: " + index);
                }
                throw ex;
            }
        }
    }
}