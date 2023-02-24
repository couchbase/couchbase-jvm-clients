/*
 * Copyright (c) 2016 Couchbase, Inc.
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
package com.couchbase.client.java.search.result;


import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.result.CoreSearchRowLocation;
import reactor.util.annotation.Nullable;

import java.util.Objects;

/**
 * A FTS result row location indicates at which position a given term occurs inside a given field.
 * In case the field is an array, {@link #arrayPositions} will indicate which index/indices in the
 * array contain the term.
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
@Stability.Volatile
public class SearchRowLocation {

    private final CoreSearchRowLocation internal;

    SearchRowLocation(CoreSearchRowLocation internal) {
        this.internal = internal;
    }

    public String field() {
        return internal.field();
    }

    public String term() {
        return internal.term();
    }

    /**
     * @return the position of the term within the field, starting at 1
     */
    public long pos() {
        return internal.pos();
    }

    /**
     * @return the start offset (in bytes) of the term in the the field
     */
    public long start() {
        return internal.start();
    }

    /**
     * @return the end offset (in bytes) of the term in the the field
     */
    public long end() {
        return internal.end();
    }

    /**
     * Contains the positions of the term within any elements.
     *
     * @return the array positions, or null if not applicable.
     */
    @Nullable public long[] arrayPositions() {
        return internal.arrayPositions();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchRowLocation searchRow = (SearchRowLocation) o;
        return Objects.equals(internal, searchRow.internal);
    }

    @Override
    public int hashCode() {
        return internal.hashCode();
    }

    @Override
    public String toString() {
        return internal.toString();
    }
}
