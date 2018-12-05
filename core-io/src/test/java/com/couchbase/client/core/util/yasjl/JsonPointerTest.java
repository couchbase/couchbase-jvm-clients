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

import com.couchbase.client.core.util.yasjl.Callbacks.JsonPointerCB1;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


/**
 * Verifies the basic functionality of the {@link JsonPointer}.
 *
 * @author Michael Nitschinger
 * @since 1.5.0
 */
class JsonPointerTest {

    @Test
    void shouldSplitTokens() {
        JsonPointer jp = new JsonPointer("/one/two/three/-");
        List<String> tokens = jp.tokens();
        assertEquals(5, tokens.size());
        assertEquals("", tokens.get(0));
        assertEquals("one", tokens.get(1));
        assertEquals("two", tokens.get(2));
        assertEquals("three", tokens.get(3));
        assertEquals("-", tokens.get(4));
        assertNull(jp.jsonPointerCB());
    }

    @Test
    void shouldSetCallback() {
        JsonPointerCB1 cb1 = value -> { };
        JsonPointerCB1 cb2 = value -> { };
        JsonPointer jp = new JsonPointer("/", cb1);
        assertEquals(cb1, jp.jsonPointerCB());

        jp.jsonPointerCB(cb2);
        assertEquals(cb2, jp.jsonPointerCB());
    }

    @Test
    void shouldReplaceSpecialChars() {
        JsonPointer jp = new JsonPointer("/foo~1bar/wh~0at");
        List<String> tokens = jp.tokens();
        assertEquals(3, tokens.size());
        assertEquals("", tokens.get(0));
        assertEquals("foo/bar", tokens.get(1));
        assertEquals("wh~at", tokens.get(2));
    }

    @Test
    void shouldReturnStringRepresentation() {
        assertEquals("JsonPointer{path=}", new JsonPointer().toString());
        assertEquals("JsonPointer{path=/foo/bar}", new JsonPointer("/foo/bar").toString());
        assertEquals("JsonPointer{path=}", new JsonPointer(new ArrayList<>()).toString());
    }

    @Test
    void shouldAddToken() {
        JsonPointer cb = new JsonPointer();
        assertFalse(cb.tokens().isEmpty());
        assertEquals("", cb.tokens().get(0));
        cb.addToken("hello");
        assertFalse(cb.tokens().isEmpty());
        assertEquals("hello", cb.tokens().get(1));
    }

    @Test
    void shouldRemoveLastToken() {
        JsonPointer jp = new JsonPointer("/one/two/three/-");
        List<String> tokens = jp.tokens();
        assertEquals(5, tokens.size());
        assertEquals("", tokens.get(0));
        assertEquals("one", tokens.get(1));
        assertEquals("two", tokens.get(2));
        assertEquals("three", tokens.get(3));
        assertEquals("-", tokens.get(4));

        jp.removeLastToken();
        assertEquals(4, jp.tokens().size());
        jp.removeLastToken();
        assertEquals(3, jp.tokens().size());

        assertEquals("", tokens.get(0));
        assertEquals("one", tokens.get(1));
        assertEquals("two", tokens.get(2));
    }

    @Test
    void shouldFailWhenNestedTooDeep() {
        StringBuilder longPath = new StringBuilder();
        for (int i = 0; i < 33; i++) {
            longPath.append("/foo");
        }
        assertThrows(IllegalArgumentException.class, () -> new JsonPointer(longPath.toString()));
    }
}