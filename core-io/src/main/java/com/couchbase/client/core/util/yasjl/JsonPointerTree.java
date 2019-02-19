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
import java.util.List;

import com.couchbase.client.core.util.yasjl.Callbacks.JsonPointerCB;

/**
 * Represents a tree structure of stored {@link JsonPointer}.
 *
 * @author Subhashni Balakrishnan
 */
public class JsonPointerTree {

    private final Node root;
    private boolean isRootAPointer;

    /**
     * Creates a new {@link JsonPointerTree}.
     */
    JsonPointerTree() {
        this.root = new Node("", null);
        this.isRootAPointer = false;
    }

    /**
     * Adds a {@link JsonPointer} to this tree.
     *
     * @return true if the {@link JsonPointer} is valid to be inserted, false otherwise.
     */
    boolean addJsonPointer(final JsonPointer jp) {
        if (isRootAPointer) {
            return false;
        }

        List<String> jpRefTokens = jp.tokens();
        int jpSize = jpRefTokens.size();
        if (jpSize == 1) {
            isRootAPointer = true;
            return true;
        }

        Node parent = root;
        boolean pathDoesNotExist = false;
        for (int i = 1; i < jpSize; i++) {
            Node childMatch = parent.match(jpRefTokens.get(i));
            if (childMatch == null) {
                parent = parent.addChild(jpRefTokens.get(i), jp.jsonPointerCB());
                pathDoesNotExist = true;
            } else {
                parent = childMatch;
            }
        }

        return pathDoesNotExist;
    }

    /**
     * Checks if the given {@link JsonPointer} represents an intermediary path in the tree.
     *
     * @param jp the pointer to check.
     * @return true if its intermediary, false otherwise (like if terminal).
     */
    boolean isIntermediaryPath(final JsonPointer jp) {
        List<String> jpRefTokens = jp.tokens();
        int jpSize = jpRefTokens.size();
        if (jpSize == 1) {
            return false;
        }

        Node node = root;
        for (int i = 1; i < jpSize; i++) {
            Node childMatch = node.match(jpRefTokens.get(i));
            if (childMatch == null) {
                return false;
            } else {
                node = childMatch;
            }
        }

        return node.children != null;
    }

    /**
     * Checks if the given {@link JsonPointer} is a terminal path in this tree.
     *
     * @param jp the pointer to check.
     * @return true if its terminal, false otherwise (like if intermediary).
     */
    boolean isTerminalPath(final JsonPointer jp) {
        List<String> jpRefTokens = jp.tokens();
        int jpSize = jpRefTokens.size();

        Node node = root;
        if (jpSize == 1) {
            if (node.children == null) {
                return false;
            }
        }

        for (int i = 1; i < jpSize; i++) {
            Node childMatch = node.match(jpRefTokens.get(i));
            if (childMatch == null) {
                return false;
            } else {
                node = childMatch;
            }
        }

        if (node != null && node.children == null) {
            jp.jsonPointerCB(node.jsonPointerCB);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "JsonPointerTree{" +
            "root=" + root +
            ", isRootAPointer=" + isRootAPointer +
            '}';
    }

    /**
     * Represents a logical Node in this JsonPointerTree.
     */
    class Node {

        private final String value;
        private final JsonPointerCB jsonPointerCB;
        private List<Node> children;

        Node(final String value, final JsonPointerCB jsonPointerCB) {
            this.value = value;
            this.children = null;
            this.jsonPointerCB = jsonPointerCB;
        }

        /**
         * Adds a child to this node and returns it.
         *
         * @param value the path for the node.
         * @param jsonPointerCB the callback to store.
         * @return the child instance created.
         */
        Node addChild(final String value, final JsonPointerCB jsonPointerCB) {
            if (children == null) {
                children = new ArrayList<Node>();
            }
            Node child = new Node(value, jsonPointerCB);
            children.add(child);
            return child;
        }

        boolean isIndex(final String s) {
            int len = s.length();
            for (int a = 0; a < len; a++) {
                if (a == 0 && s.charAt(a) == '-') continue;
                if (!Character.isDigit(s.charAt(a))) return false;
            }
            return true;
        }

        /**
         * Returns the node which matches the given input.
         *
         * @param value the value to match against.
         * @return the node if found, null otherwise.
         */
        public Node match(final String value) {
            if (this.children == null) {
                return null;
            }

            for (Node child : children) {
                if (child.value.equals(value)) {
                    return child;
                } else if ((child.value.equals("-") && isIndex(value))) {
                    return child;
                }
            }
            return null;
        }

        @Override
        public String toString() {
            return "Node{" +
                "value='" + value + '\'' +
                ", jsonPointerCB=" + jsonPointerCB +
                ", children=" + children +
                '}';
        }
    }
}