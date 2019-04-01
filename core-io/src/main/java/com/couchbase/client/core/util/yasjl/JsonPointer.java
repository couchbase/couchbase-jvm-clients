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
import java.util.Collections;
import java.util.List;

/**
 * Represents a pointer in the JSON tree structure.
 *
 * @author Subhashni Balakrishnan
 */
public class JsonPointer {

    private static final int MAX_NESTING_LEVEL = 31;
    private static final String ROOT_TOKEN = "";

    private final List<String> refTokens;
    private JsonPointerCallback callback;

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
        this.refTokens = new ArrayList<>(tokens);
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
    public JsonPointer(final String path, final JsonPointerCallback callback) {
        this.refTokens = new ArrayList<>();
        this.callback = callback;
        parseComponents(path);
    }

    /**
     * Helper method to split up the path into individual components (tokens).
     *
     * @param path the path split up into tokens subsequently.
     */
    private void parseComponents(final String path) {
        String[] components = path.split("/");

        if (components.length > MAX_NESTING_LEVEL) {
            throw new IllegalArgumentException("Provided path contains too many levels of nesting." +
              " Max is " + MAX_NESTING_LEVEL + " but got " + components.length);
        }

        for (String c : components) {
            this.refTokens.add(unescape(c));
        }
    }

    private static String unescape(String component) {
        return component.replace("~1","/").replace("~0","~");
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
    JsonPointerCallback callback() {
        return this.callback;
    }

    /**
     * Allows to set the callback explicitly, can also be set to null to "unset".
     *
     * @param callback the callback to store.
     */
    void callback(final JsonPointerCallback callback) {
        this.callback = callback;
    }

    @Override
    public String toString() {
        return "JsonPointer{path=" + String.join("/", this.refTokens) + "}";
    }

}
