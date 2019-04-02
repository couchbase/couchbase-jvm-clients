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

import static com.couchbase.client.core.util.yasjl.JsonParserUtils.WS_CR;
import static com.couchbase.client.core.util.yasjl.JsonParserUtils.WS_LF;
import static com.couchbase.client.core.util.yasjl.JsonParserUtils.WS_SPACE;
import static com.couchbase.client.core.util.yasjl.JsonParserUtils.WS_TAB;

/**
 * Processes JSON ws
 *
 * @author Subhashni Balakrishnan
 */
public class JsonWhiteSpaceByteBufProcessor implements ByteProcessor {

    public boolean process(byte value) throws Exception {
        switch(value) {
            case WS_SPACE:
            case WS_TAB:
            case WS_LF:
            case WS_CR:
                return true;
            default:
                return false;
        }
    }
}
