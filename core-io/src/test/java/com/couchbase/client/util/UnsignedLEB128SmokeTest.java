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

package com.couchbase.client.util;

import static org.junit.jupiter.api.Assertions.*;
import java.math.BigInteger;
import com.couchbase.client.core.util.UnsignedLEB128;
import org.junit.jupiter.api.Test;

/**
 * Smoke tests for unsigned little endian base 128
 *
 */
public class UnsignedLEB128SmokeTest {

    @Test
    public void testEncoding1() {
        byte[] res = UnsignedLEB128.encode(BigInteger.valueOf(555));
        int[] ires = convert(res);
        assertEquals(2, res.length,"Array length mismatch");
        assertEquals("4", Integer.toString(ires[0], 16));
        assertEquals("ab", Integer.toString(ires[1], 16));
    }

    @Test
    public void testDecoding1() {
        BigInteger n = BigInteger.valueOf(555);
        byte[] encoded = UnsignedLEB128.encode(n);
        BigInteger res = UnsignedLEB128.decode(encoded);
        assertEquals(res, n);
    }

    @Test
    public void testEncoding2() {
        byte[] res = UnsignedLEB128.encode(new BigInteger("cafef00", 16));
        int[] ires = convert(res);
        assertEquals(4, res.length,"Array length mismatch");
        assertEquals("65", Integer.toString(ires[0], 16));
        assertEquals("bf", Integer.toString(ires[1], 16));
        assertEquals("de", Integer.toString(ires[2], 16));
        assertEquals("80", Integer.toString(ires[3], 16));
    }

    @Test
    public void testDecoding2() {
        BigInteger n = new BigInteger("cafef00", 16);
        byte[] encoded = UnsignedLEB128.encode(n);
        BigInteger res = UnsignedLEB128.decode(encoded);
        assertTrue(n.compareTo(res) == 0);
    }

    @Test
    public void testEncoding3() {
        byte[] res = UnsignedLEB128.encode(new BigInteger("cafef00d", 16));
        int[] ires = convert(res);
        assertEquals(5, res.length,"Array length mismatch");
        assertEquals("c", Integer.toString(ires[0], 16));
        assertEquals("d7", Integer.toString(ires[1], 16));
        assertEquals("fb", Integer.toString(ires[2], 16));
        assertEquals("e0", Integer.toString(ires[3], 16));
        assertEquals("8d", Integer.toString(ires[4], 16));
    }

    @Test
    public void testDecoding3() {
        BigInteger n = new BigInteger("cafef00d", 16);
        byte[] encoded = UnsignedLEB128.encode(n);
        BigInteger res = UnsignedLEB128.decode(encoded);
        assertTrue(n.compareTo(res) == 0);
    }

    @Test
    public void testEncoding4() {
        byte[] res = UnsignedLEB128.encode(new BigInteger("7fff", 16));
        int[] ires = new int[res.length];
        for (int i=0;i<res.length;i++) {
            ires[i] = res[i];
            if (ires[i] < 0) ires[i] += 256;
        }
        assertEquals(3, res.length,"Array length mismatch");
        assertEquals("1", Integer.toString(ires[0], 16));
        assertEquals("ff", Integer.toString(ires[1], 16));
        assertEquals("ff", Integer.toString(ires[2], 16));
    }

    @Test
    public void testDecoding4() {
        BigInteger n = new BigInteger("7fff", 16);
        byte[] encoded = UnsignedLEB128.encode(n);
        BigInteger res = UnsignedLEB128.decode(encoded);
        assertTrue(n.compareTo(res) == 0);
    }

    @Test
    public void testEncoding5() {
        byte[] res = UnsignedLEB128.encode(new BigInteger("bfff", 16));
        int[] ires = convert(res);
        assertEquals(3, res.length,"Array length mismatch");
        assertEquals("2", Integer.toString(ires[0], 16));
        assertEquals("ff", Integer.toString(ires[1], 16));
        assertEquals("ff", Integer.toString(ires[2], 16));
    }

    @Test
    public void testDecoding5() {
        BigInteger n = new BigInteger("bfff", 16);
        byte[] encoded = UnsignedLEB128.encode(n);
        BigInteger res = UnsignedLEB128.decode(encoded);
        assertTrue(n.compareTo(res) == 0);
    }

    @Test
    public void testEncoding6() {
        byte[] res = UnsignedLEB128.encode(new BigInteger("ffff", 16));
        int[] ires = convert(res);
        assertEquals(3, res.length,"Array length mismatch");
        assertEquals("3", Integer.toString(ires[0], 16));
        assertEquals("ff", Integer.toString(ires[1], 16));
        assertEquals("ff", Integer.toString(ires[2], 16));
    }

    @Test
    public void testDecoding6() {
        BigInteger n = new BigInteger("ffff", 16);
        byte[] encoded = UnsignedLEB128.encode(n);
        BigInteger res = UnsignedLEB128.decode(encoded);
        assertTrue(n.compareTo(res) == 0);
    }

    @Test
    public void testEncoding7() {
        byte[] res = UnsignedLEB128.encode(new BigInteger("8000", 16));
        int[] ires = convert(res);
        assertEquals(3, res.length,"Array length mismatch");
        assertEquals("2", Integer.toString(ires[0], 16));
        assertEquals("80", Integer.toString(ires[1], 16));
        assertEquals("80", Integer.toString(ires[2], 16));
    }

    @Test
    public void testDecoding7() {
        BigInteger n = new BigInteger("8000", 16);
        byte[] encoded = UnsignedLEB128.encode(n);
        BigInteger res = UnsignedLEB128.decode(encoded);
        assertTrue(n.compareTo(res) == 0);
    }

    @Test
    public void testEncoding8() {
        byte[] res = UnsignedLEB128.encode(new BigInteger("5555", 16));
        int[] ires = convert(res);
        assertEquals(3, res.length,"Array length mismatch");
        assertEquals("1", Integer.toString(ires[0], 16));
        assertEquals("aa", Integer.toString(ires[1], 16));
        assertEquals("d5", Integer.toString(ires[2], 16));
    }

    @Test
    public void testDecoding8() {
        BigInteger n = new BigInteger("5555", 16);
        byte[] encoded = UnsignedLEB128.encode(n);
        BigInteger res = UnsignedLEB128.decode(encoded);
        assertTrue(n.compareTo(res) == 0);
    }

    @Test
    public void testEncoding9() {
        byte[] res = UnsignedLEB128.encode(new BigInteger("ffffffff", 16));
        int[] ires = convert(res);
        assertEquals(5, res.length,"Array length mismatch");
        assertEquals("f", Integer.toString(ires[0], 16));
        assertEquals("ff", Integer.toString(ires[1], 16));
        assertEquals("ff", Integer.toString(ires[2], 16));
        assertEquals("ff", Integer.toString(ires[3], 16));
        assertEquals("ff", Integer.toString(ires[4], 16));
    }

    @Test
    public void testDecoding9() {
        BigInteger n = new BigInteger("ffffffff", 16);
        byte[] encoded = UnsignedLEB128.encode(n);
        BigInteger res = UnsignedLEB128.decode(encoded);
        assertTrue(n.compareTo(res) == 0);
    }

    @Test
    public void testEncoding10() {
        byte[] res = UnsignedLEB128.encode(new BigInteger("00", 16));
        int[] ires = convert(res);
        assertEquals(1, res.length,"Array length mismatch");
        assertEquals("0", Integer.toString(ires[0], 16));
    }

    @Test
    public void testDecoding10() {
        BigInteger n = new BigInteger("00", 16);
        byte[] encoded = UnsignedLEB128.encode(n);
        BigInteger res = UnsignedLEB128.decode(encoded);
        assertTrue(n.compareTo(res) == 0);
    }

    @Test
    public void testEncoding11() {
        byte[] res = UnsignedLEB128.encode(new BigInteger("01", 16));
        int[] ires = convert(res);
        assertEquals(1, res.length,"Array length mismatch");
        assertEquals("1", Integer.toString(ires[0], 16));
    }

    @Test
    public void testDecoding11() {
        BigInteger n = new BigInteger("01", 16);
        byte[] encoded = UnsignedLEB128.encode(n);
        BigInteger res = UnsignedLEB128.decode(encoded);
        assertTrue(n.compareTo(res) == 0);
    }

    @Test
    public void testEncoding12() {
        byte[] res = UnsignedLEB128.encode(new BigInteger("7f", 16));
        int[] ires = convert(res);
        assertEquals(1, res.length,"Array length mismatch");
        assertEquals("7f", Integer.toString(ires[0], 16));
    }

    @Test
    public void testDecoding12() {
        BigInteger n = new BigInteger("7F", 16);
        byte[] encoded = UnsignedLEB128.encode(n);
        BigInteger res = UnsignedLEB128.decode(encoded);
        assertTrue(n.compareTo(res) == 0);
    }

    @Test
    public void testEncoding13() {
        byte[] res = UnsignedLEB128.encode(new BigInteger("80", 16));
        int[] ires = convert(res);
        assertEquals(2, res.length,"Array length mismatch");
        assertEquals("1", Integer.toString(ires[0], 16));
        assertEquals("80", Integer.toString(ires[1], 16));
    }

    @Test
    public void testDecoding13() {
        BigInteger n = new BigInteger("80", 16);
        byte[] encoded = UnsignedLEB128.encode(n);
        BigInteger res = UnsignedLEB128.decode(encoded);
        assertTrue(n.compareTo(res) == 0);
    }

    private int[] convert(byte[] res) {
        int[] ires = new int[res.length];
        for (int i=0;i<res.length;i++) {
            ires[i] = res[i];
            if (ires[i] < 0) ires[i] += 256;
        }
        return ires;
    }
}