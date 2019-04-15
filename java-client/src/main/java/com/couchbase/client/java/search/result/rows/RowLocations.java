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
package com.couchbase.client.java.search.result.rows;

import com.couchbase.client.core.annotation.Stability;

import java.util.List;
import java.util.Set;

/**
 * Represents the locations of a search result row. {@link RowLocation locations} show
 * where a given term occurs inside of a given field.
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
@Stability.Volatile
public interface RowLocations {

    /** add a location and allow method chaining */
    RowLocations add(RowLocation l);

    /**list all locations for a given field (any term) */
    List<RowLocation> get(String field);

    /**list all locations for a given field and term */
    List<RowLocation> get(String field, String term);

    /**list all locations (any field, any term) */
    List<RowLocation> getAll();

    /**size of all() */
    long count();

    /**list the fields in this location */
    List<String> fields();

    /**list the terms for a given field */
    List<String> termsFor(String field);

    /**list all terms in this locations, considering all fields (so a set) */
    Set<String> terms();
}
