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

package com.couchbase.client.java.search;

import com.couchbase.client.core.deps.io.netty.util.ResourceLeakDetector;
import com.couchbase.client.core.io.netty.search.SearchMock;
import com.couchbase.client.java.search.result.SearchResult;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class SearchTest {

    static {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @Test
    void alltimeouts() throws ExecutionException, InterruptedException, IOException {
        InputStream json = getClass().getClassLoader().getResourceAsStream("sdk-testcases/search/alltimeouts.json");
        SearchResult result = SearchMock.loadSearchTestCase(json);

        assertEquals(6, result.metaData().errors().size());
        assertEquals(6, result.metaData().metrics().errorPartitionCount());
        assertEquals(6, result.metaData().metrics().totalPartitionCount());
        assertEquals(0, result.metaData().metrics().successPartitionCount());
    }

}
