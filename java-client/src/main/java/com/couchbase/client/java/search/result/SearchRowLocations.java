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
import com.couchbase.client.core.api.search.result.CoreSearchRowLocations;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A default implementation of a {@link SearchRowLocations}.
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
@Stability.Volatile
public class SearchRowLocations {

    private final CoreSearchRowLocations internal;
    private int size;

    @Stability.Internal
    public SearchRowLocations(CoreSearchRowLocations locations) {
        this.internal = locations;
    }

    private static List<SearchRowLocation> convert(List<CoreSearchRowLocation> locations) {
        return locations.stream().map(SearchRowLocation::new).collect(Collectors.toList());
    }

    public List<SearchRowLocation> get(String field) {
        return convert(internal.get(field));
    }

    public List<SearchRowLocation> get(String field, String term) {
        return convert(internal.get(field, term));
    }

    public List<SearchRowLocation> getAll() {
        return convert(internal.getAll());
    }

    public List<String> fields() {
        return internal.fields();
    }

    public List<String> termsFor(String field) {
        return internal.termsFor(field);
    }

    public Set<String> terms() {
        return internal.terms();
    }

    @Override
    public String toString() {
        return internal.toString();
    }
}
