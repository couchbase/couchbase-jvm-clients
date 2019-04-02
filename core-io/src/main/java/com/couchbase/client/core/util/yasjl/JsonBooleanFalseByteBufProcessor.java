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

import com.couchbase.client.core.deps.io.netty.util.ByteProcessor;

/**
 * Processes JSON false value
 *
 * @author Subhashni Balakrishnan
 */
public class JsonBooleanFalseByteBufProcessor implements ByteProcessor {
    private static final byte F1 = (byte)'f';
    private static final byte F2 = (byte)'a';
    private static final byte F3 = (byte)'l';
    private static final byte F4 = (byte)'s';
    private static final byte F5 = (byte)'e';

    private byte lastValue;

    public JsonBooleanFalseByteBufProcessor() {
        reset();
    }

    public void reset() {
        this.lastValue = F1;
    }

    public boolean process(byte value) throws Exception {
        switch (value) {
            case F2:
                if (this.lastValue == F1) {
                    this.lastValue = F2;
                    return true;
                }
                break;
            case F3:
                if (this.lastValue == F2) {
                    this.lastValue = F3;
                    return true;
                }
                break;
            case F4:
                if (this.lastValue == F3) {
                    this.lastValue = F4;
                    return true;
                }
                break;
            case F5:
                if (this.lastValue == F4) {
                    reset();
                    return false;
                }
                break;
        }
        return false;
    }
}
