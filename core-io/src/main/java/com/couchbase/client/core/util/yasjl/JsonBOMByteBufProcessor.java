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
 * Processes byte order mark. It supports only UTF-8.
 *
 * @author Subhashni Balakrishnan
 */
public class JsonBOMByteBufProcessor implements ByteProcessor {
    private static final byte BOM1 = (byte)0xEF;
    private static final byte BOM2 = (byte)0xBB;
    private static final byte BOM3 = (byte)0xBF;
    private byte lastValue;

    public JsonBOMByteBufProcessor() {
        reset();
    }

    private void reset() {
        this.lastValue = BOM1;
    }

    public boolean process(byte value) throws Exception {
        switch(value) {
            case BOM2:
                if (this.lastValue == BOM1) {
                    this.lastValue = BOM2;
                    return true;
                }
                break;
            case BOM3:
                if (this.lastValue == BOM2) {
                    return false;
                }
                break;
        }
        return false;
    }
}
