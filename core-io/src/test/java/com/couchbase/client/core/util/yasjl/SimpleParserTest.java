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

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.util.ResourceLeakDetector;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Subhashni Balakrishnan
 */
class SimpleParserTest {

    static {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }


    private Map<String, Object> writeSimpleJsonAndParse(String path) {
        ByteBuf inBuf = Unpooled.buffer();

        final Map<String, Object> results = new HashMap<String, Object>();
        final int[] parseCount = new int[1];
        parseCount[0] = 0;
        JsonPointer[] jp = { new JsonPointer(path, buf -> {
            results.put("value", buf.toString(Charset.defaultCharset()));
            results.put("parseCount", parseCount[0]);
            buf.release();
        })};
        ByteBufJsonParser parser = new ByteBufJsonParser(jp);
        parser.initialize(inBuf);

        try {
            inBuf.writeBytes("{\"foo\": [\"bar\", \"baz\"],".getBytes());
            parseCount[0]++;
            parser.parse();
            inBuf.discardReadBytes();
        } catch(EOFException ex) {
        }

        try {
            inBuf.writeBytes("\"\":0,".getBytes());
            parseCount[0]++;
            parser.parse();
            inBuf.discardReadBytes();
        } catch(EOFException ex) {
        }

        try {
            inBuf.writeBytes("\"a/b\": 1,".getBytes());
            parseCount[0]++;
            parser.parse();
            inBuf.discardReadBytes();
        } catch(EOFException ex) {
        }

        try {
            inBuf.writeBytes("\"c%d\": 2,".getBytes());
            parseCount[0]++;
            parser.parse();
            inBuf.discardReadBytes();
        } catch(EOFException ex) {
        }

        try {
            inBuf.writeBytes("\"e^f\": 3,".getBytes());
            parseCount[0]++;
            parser.parse();
            inBuf.discardReadBytes();
        } catch(EOFException ex) {
        }

        try {
            inBuf.writeBytes("\"g|h\": 4,".getBytes());
            parseCount[0]++;
            parser.parse();
            inBuf.discardReadBytes();
        } catch(EOFException ex) {
        }

        try {
            inBuf.writeBytes("\"i\\j\": 5,".getBytes());
            parseCount[0]++;
            parser.parse();
            inBuf.discardReadBytes();
        } catch(EOFException ex) {
        }


        try {
            inBuf.writeBytes("\"k\\\"l\": 6,".getBytes());
            parseCount[0]++;
            parser.parse();
            inBuf.discardReadBytes();
        } catch(EOFException ex) {
        }

        try {
            inBuf.writeBytes("\" \": 7,".getBytes());
            parseCount[0]++;
            parser.parse();
            inBuf.discardReadBytes();
        } catch(EOFException ex) {

        }

        try {
            inBuf.writeBytes("\"m~n\": 8}".getBytes());
            parseCount[0]++;
            parser.parse();
            inBuf.discardReadBytes();
        } catch(EOFException ex) {

        }

        return results;
    }

    @Test
    void testJsonArrayPointerValue() {
        Map<String, Object> results = writeSimpleJsonAndParse("/foo");
        assertEquals("[\"bar\", \"baz\"]", results.get("value").toString());
        assertEquals(1, results.get("parseCount"));
    }

    @Test
    void testJsonArrayElementPointerValue() {
        Map<String, Object> results = writeSimpleJsonAndParse("/foo/0");
        assertEquals("\"bar\"", results.get("value").toString());
        assertEquals(1, results.get("parseCount"));
    }

    @Test
    void testEscapedPathValue() {
        Map<String, Object> results = writeSimpleJsonAndParse("/a~1b");
        assertEquals("1", results.get("value").toString());
        assertEquals(3, results.get("parseCount"));
    }

    @Test
    void testSpecialCharPathValue_1() {
        Map<String, Object> results = writeSimpleJsonAndParse("/c%d");
        assertEquals("2", results.get("value").toString());
        assertEquals(4, results.get("parseCount"));
    }

    @Test
    void testSpecialCharPathValue_2() {
        Map<String, Object> results = writeSimpleJsonAndParse("/e^f");
        assertEquals("3", results.get("value").toString());
        assertEquals(5, results.get("parseCount"));
    }


    @Test
    void testSpecialCharPathValue_3() {
        Map<String, Object> results = writeSimpleJsonAndParse("/g|h");
        assertEquals("4", results.get("value").toString());
        assertEquals(6, results.get("parseCount"));
    }

    @Test
    void testSpecialCharPathValue_4() {
        Map<String, Object> results = writeSimpleJsonAndParse("/i\\j");
        assertEquals("5", results.get("value").toString());
        assertEquals(7, results.get("parseCount"));
    }

    @Test
    void testSpecialCharPathValue_5() {
        Map<String, Object> results = writeSimpleJsonAndParse("/k\\\"l");
        assertEquals("6", results.get("value").toString());
        assertEquals(8, results.get("parseCount"));
    }

    @Test
    void testSpecialCharPathValue_6() {
        Map<String, Object> results = writeSimpleJsonAndParse("/ ");
        assertEquals("7", results.get("value").toString());
        assertEquals(9, results.get("parseCount"));
    }


    @Test
    void testSpecialCharPathValue_7() {
        Map<String, Object> results = writeSimpleJsonAndParse("/m~n");
        assertEquals("8", results.get("value").toString());
        assertEquals(10, results.get("parseCount"));
    }

}
