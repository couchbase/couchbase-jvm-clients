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

/**
 * @author Subhashni Balakrishnan
 */
public class JsonParserUtils {

    protected enum Mode {
        JSON_OBJECT,
        JSON_OBJECT_VALUE,
        JSON_ARRAY,
        JSON_ARRAY_VALUE,
        JSON_STRING_HASH_KEY,
        JSON_STRING_VALUE,
        JSON_BOOLEAN_TRUE_VALUE,
        JSON_BOOLEAN_FALSE_VALUE,
        JSON_NUMBER_VALUE,
        JSON_NULL_VALUE,
        BOM //byte order mark
    }

    protected static final byte O_CURLY = (byte)'{';
    protected static final byte C_CURLY = (byte)'}';
    protected static final byte O_SQUARE = (byte)'[';
    protected static final byte C_SQUARE = (byte)']';

    protected static final byte JSON_ST = (byte)'"';
    protected static final byte JSON_T = (byte)'t';
    protected static final byte JSON_F = (byte)'f';
    protected static final byte JSON_N = (byte)'n';
    protected static final byte JSON_ES = (byte)'\\';
    protected static final byte JSON_COLON = (byte)':';
    protected static final byte JSON_COMMA = (byte)',';

    protected static final byte JSON_MINUS = (byte)'-';
    protected static final byte JSON_PLUS = (byte)'+';
    protected static final byte JSON_ZERO = (byte)'0';

    protected static final byte WS_SPACE = (byte)0x20;
    protected static final byte WS_TAB = (byte)0X09;
    protected static final byte WS_LF = (byte)0x0A;
    protected static final byte WS_CR = (byte)0x0D;


    public static boolean isNumber(final byte value) {
        switch(value) {
            case JSON_MINUS:
            case JSON_ZERO:
            case JSON_PLUS:
                return true;
            default:
                return value >= (byte) '1' && value <= (byte) '9';
        }
    }
}