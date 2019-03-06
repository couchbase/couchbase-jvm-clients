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

import static com.couchbase.client.core.util.yasjl.JsonParserUtils.*;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBufProcessor;

/**
 * Processes JSON String value
 *
 * @author Subhashni Balakrishnan
 */
public class JsonStringByteBufProcessor implements ByteBufProcessor {
    private State currentState;

    private enum State {
        UNESCAPED, ESCAPED
    }

    public JsonStringByteBufProcessor() {
        reset();
    }

    public void reset(){
        this.currentState = State.UNESCAPED;
    }

    public boolean process(byte value) throws Exception {
        switch(value) {
            case JSON_ES:
                if (this.currentState == State.UNESCAPED) {
                    this.currentState = State.ESCAPED;
                } else {
                    this.currentState = State.UNESCAPED;
                }
                return true;
            case JSON_ST:
                if (this.currentState == State.ESCAPED) {
                    this.currentState = State.UNESCAPED;
                    return true;
                } else {
                    reset();
                    return false;
                }
            default:
                if (this.currentState == State.ESCAPED) {
                    this.currentState = State.UNESCAPED;
                }
                return true;
        }
    }
}