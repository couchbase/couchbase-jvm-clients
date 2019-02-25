/*
 * Copyright (c) 2017 Couchbase, Inc.
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
package com.couchbase.client.core.util.yasjl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.couchbase.client.core.util.yasjl.Callbacks.JsonPointerCB;

/**
 * Represents a pointer in the JSON tree structure.
 *
 * @author Subhashni Balakrishnan
 */
public class JsonPointer {

    private static final int MAX_NESTING_LEVEL = 31;
    private static final String ROOT_TOKEN = "";

    private final List<String> refTokens;
    private JsonPointerCB jsonPointerCB;

    /**
     * Creates a new {@link JsonPointer} with just the {@link #ROOT_TOKEN} as the path.
     */
    JsonPointer() {
        this(Collections.singletonList(ROOT_TOKEN));
    }

    /**
     * Creates a new {@link JsonPointer} with a given list of tokens.
     *
     * @param tokens the list of tokens to use directly.
     */
    JsonPointer(final List<String> tokens) {
        this.refTokens = new ArrayList<String>(tokens);
    }

    /**
     * Creates a new {@link JsonPointer} with a path but no callback.
     *
     * @param path the path split up into tokens subsequently.
     */
    JsonPointer(final String path) {
        this(path, null);
    }

    /**
     * Creates a new {@link JsonPointer} with a path and a callback.
     *
     * @param path the path split up into tokens subsequently.
     * @param callback the callback to use for this pointer.
     */
    public JsonPointer(final String path, final JsonPointerCB callback) {
        this.refTokens = new ArrayList<String>();
        this.jsonPointerCB = callback;
        parseComponents(path);
    }

    /**
     * Helper method to split up the path into individual components (tokens).
     *
     * @param path the path split up into tokens subsequently.
     */
    private void parseComponents(final String path) {
        String[] tokens = path.split("/");

        if (tokens.length > MAX_NESTING_LEVEL) {
            throw new IllegalArgumentException("Provided path contains too many levels of nesting!");
        }

        //replace ~1 and ~0
        for (int i=0; i < tokens.length; i++) {
            tokens[i] = tokens[i].replace("~1","/").replace("~0","~");
        }

        this.refTokens.addAll(Arrays.asList(tokens));
    }

    /**
     * Add a token to the current token list.
     *
     * @param token the string token to add.
     */
    void addToken(final String token) {
        this.refTokens.add(token);
    }

    /**
     * Removes the last token from the current token list if there is at least one
     * token left (the root token).
     */
    void removeLastToken() {
        if (this.refTokens.size() > 1) {
            this.refTokens.remove(this.refTokens.size() - 1);
        }
    }

    /**
     * Returns the list of currently stored tokens.
     *
     * @return the list of tokens.
     */
    protected List<String> tokens() {
        return this.refTokens;
    }

    /**
     * Returns the current set json pointer callback.
     *
     * @return the callback if set, null otherwise.
     */
    JsonPointerCB jsonPointerCB() {
        return this.jsonPointerCB;
    }

    /**
     * Allows to set the callback explicitly, can also be set to null to "unset".
     *
     * @param callback the callback to store.
     */
    void jsonPointerCB(final JsonPointerCB callback) {
        this.jsonPointerCB = callback;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(String refToken : this.refTokens) {
            sb.append("/");
            sb.append(refToken);
        }
        return "JsonPointer{path=" + (this.refTokens.isEmpty() ? "" : sb.substring(1)) + "}";
    }

}