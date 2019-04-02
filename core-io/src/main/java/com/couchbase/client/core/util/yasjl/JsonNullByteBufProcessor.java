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
 * Processes JSON null value
 *
 * @author Subhashni Balakrishnan
 */
public class JsonNullByteBufProcessor implements ByteProcessor {
    private static final byte N1 = (byte)'n';
    private static final byte N2 = (byte)'u';
    private static final byte N3 = (byte)'l';
    private byte lastValue;

    public JsonNullByteBufProcessor() {
        reset();
    }

    public void reset() {
        this.lastValue = N1;
    }

    public boolean process(byte value) throws Exception {
        switch (value) {
            case N2:
                if (this.lastValue == N1) {
                    this.lastValue = N2;
                    return true;
                }
                break;
            case N3:
                if (this.lastValue == N2) {
                    this.lastValue = N3;
                    return true;
                } else if (this.lastValue == N3) {
                    return false;
                }
                break;
        }
        return false;
    }
}
