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
import com.couchbase.client.core.deps.io.netty.util.ByteProcessor;

import java.io.EOFException;
import java.util.ArrayDeque;
import java.util.Deque;

import static com.couchbase.client.core.util.yasjl.JsonParserUtils.C_CURLY;
import static com.couchbase.client.core.util.yasjl.JsonParserUtils.C_SQUARE;
import static com.couchbase.client.core.util.yasjl.JsonParserUtils.JSON_COLON;
import static com.couchbase.client.core.util.yasjl.JsonParserUtils.JSON_COMMA;
import static com.couchbase.client.core.util.yasjl.JsonParserUtils.JSON_F;
import static com.couchbase.client.core.util.yasjl.JsonParserUtils.JSON_N;
import static com.couchbase.client.core.util.yasjl.JsonParserUtils.JSON_ST;
import static com.couchbase.client.core.util.yasjl.JsonParserUtils.JSON_T;
import static com.couchbase.client.core.util.yasjl.JsonParserUtils.Mode;
import static com.couchbase.client.core.util.yasjl.JsonParserUtils.Mode.JSON_NUMBER_VALUE;
import static com.couchbase.client.core.util.yasjl.JsonParserUtils.O_CURLY;
import static com.couchbase.client.core.util.yasjl.JsonParserUtils.O_SQUARE;
import static com.couchbase.client.core.util.yasjl.JsonParserUtils.isNumber;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The {@link ByteBufJsonParser} allows to query for values identified by {@link JsonPointer} in Netty {@link ByteBuf}.
 *
 * A couple of notes:
 *  - it strictly works on UTF-8
 *  - it is not a json validator
 *  - it parses up to the given {@link JsonPointer} paths and returns their value
 *  - it is not thread safe!
 *
 * @author Subhashni Balakrishnan
 */
public class ByteBufJsonParser {

    private static final EOFException NEED_MORE_DATA = new EOFException();
    static {
        NEED_MORE_DATA.setStackTrace(new StackTraceElement[0]);
    }

    private final JsonPointerTree tree;
    private final Deque<JsonLevel> levelStack;
    private final JsonWhiteSpaceByteBufProcessor wsProcessor;
    private final JsonStringByteBufProcessor stProcessor;
    private final JsonArrayByteBufProcessor arProcessor;
    private final JsonObjectByteBufProcessor obProcessor;
    private final JsonNullByteBufProcessor nullProcessor;
    private final JsonBOMByteBufProcessor bomProcessor;
    private final JsonNumberByteBufProcessor numProcessor;
    private final JsonBooleanTrueByteBufProcessor trueProcessor;
    private final JsonBooleanFalseByteBufProcessor falseProcessor;

    private ByteBuf content;
    private byte currentChar;
    private boolean startedStreaming;

    /**
     * Creates a new {@link ByteBufJsonParser} and initializes all of its internal processors.
     *
     * @param jsonPointers the pointers which should be set.
     */
    public ByteBufJsonParser(final JsonPointer[] jsonPointers) {
        wsProcessor = new JsonWhiteSpaceByteBufProcessor();
        stProcessor = new JsonStringByteBufProcessor();
        arProcessor = new JsonArrayByteBufProcessor(stProcessor);
        obProcessor = new JsonObjectByteBufProcessor(stProcessor);
        nullProcessor = new JsonNullByteBufProcessor();
        bomProcessor = new JsonBOMByteBufProcessor();
        numProcessor = new JsonNumberByteBufProcessor();
        trueProcessor = new JsonBooleanTrueByteBufProcessor();
        falseProcessor = new JsonBooleanFalseByteBufProcessor();
        levelStack = new ArrayDeque<JsonLevel>();
        tree = new JsonPointerTree();

        for (JsonPointer jp : jsonPointers) {
            //ignores if the json pointers were actually inserted, whatever is valid gets inserted
            tree.addJsonPointer(jp);
        }
    }

    /**
     * (re)initializes this parser with new content.
     *
     * @param content the content used for parsing.
     */
    public void initialize(final ByteBuf content) {
        this.content = content;
        startedStreaming = false;
    }

    /**
     * Instructs the parser to start parsing the current buffer.
     *
     * @throws EOFException if parsing fails.
     */
    public void parse() throws EOFException {
        if (!startedStreaming && levelStack.isEmpty()) {
            readNextChar(null);

            switch (currentChar) {
                case (byte) 0xEF:
                    pushLevel(Mode.BOM);
                    break;
                case O_CURLY:
                    pushLevel(Mode.JSON_OBJECT);
                    break;
                case O_SQUARE:
                    pushLevel(Mode.JSON_ARRAY);
                    break;
                default:
                    throw new IllegalStateException("Only UTF-8 is supported");
            }

            startedStreaming = true;
        }

        while (true) {
            if (levelStack.isEmpty()) {
                return; //nothing more to do
            }

            JsonLevel currentLevel = levelStack.peek();
            switch (currentLevel.peekMode()) {
                case BOM:
                    readBOM();
                    break;
                case JSON_OBJECT:
                    readObject(currentLevel);
                    break;
                case JSON_ARRAY:
                    readArray(currentLevel);
                    break;
                case JSON_OBJECT_VALUE:
                case JSON_ARRAY_VALUE:
                case JSON_STRING_HASH_KEY:
                case JSON_STRING_VALUE:
                case JSON_BOOLEAN_TRUE_VALUE:
                case JSON_BOOLEAN_FALSE_VALUE:
                case JSON_NUMBER_VALUE:
                case JSON_NULL_VALUE:
                    readValue(currentLevel);
                    break;
            }
        }
    }

    /**
     * Pushes a new {@link JsonLevel} onto the level stack.
     *
     * @param mode the mode for this level.
     */
    private void pushLevel(final Mode mode) {
        JsonLevel newJsonLevel = null;

        if (mode == Mode.BOM) {
            newJsonLevel = new JsonLevel(mode, new JsonPointer()); //not a valid nesting level
        } else if (mode == Mode.JSON_OBJECT) {
            if (levelStack.size() > 0) {
                JsonLevel current = levelStack.peek();
                newJsonLevel = new JsonLevel(mode, new JsonPointer(current.jsonPointer().tokens()));
            } else {
                newJsonLevel = new JsonLevel(mode, new JsonPointer());
            }
        } else if (mode == Mode.JSON_ARRAY) {
            if (levelStack.size() > 0) {
                JsonLevel current = levelStack.peek();
                newJsonLevel = new JsonLevel(mode, new JsonPointer(current.jsonPointer().tokens()));
            } else {
                newJsonLevel = new JsonLevel(mode, new JsonPointer());
            }
            newJsonLevel.isArray(true);
            newJsonLevel.setArrayIndexOnJsonPointer();
        }

        levelStack.push(newJsonLevel);
    }

    /**
     * Helper method to clean up after being done with the last stack
     * so it removes the tokens that pointed to that object.
     */
    private void popAndResetToOldLevel() {
        this.levelStack.pop();
        if (!this.levelStack.isEmpty()) {
            JsonLevel newTop = levelStack.peek();
            if (newTop != null) {
                newTop.removeLastTokenFromJsonPointer();
            }
        }
    }

    /**
     * Handle the logic for reading down a JSON Object.
     *
     * @param level the current level.
     * @throws EOFException if more data is needed.
     */
    private void readObject(final JsonLevel level) throws EOFException {
        while (true) {
            readNextChar(level);
            if (this.currentChar == JSON_ST) {
                if (!level.isHashValue()) {
                    level.pushMode(Mode.JSON_STRING_HASH_KEY);
                } else {
                    level.pushMode(Mode.JSON_STRING_VALUE);
                }
                readValue(level);
            } else if (this.currentChar == JSON_COLON) {
                //look for value
                level.isHashValue(true);
            } else if (this.currentChar == O_CURLY) {
                if (!level.isHashValue()) {
                    throw new IllegalStateException("Invalid json, json object can only be a hash value not key");
                }
                if (this.tree.isIntermediaryPath(level.jsonPointer())) {
                    this.pushLevel(Mode.JSON_OBJECT);
                    level.removeLastTokenFromJsonPointer();
                    return;
                }
                level.pushMode(Mode.JSON_OBJECT_VALUE);
                readValue(level);
            } else if (this.currentChar == O_SQUARE) {
                if (!level.isHashValue()) {
                    throw new IllegalStateException("Invalid json, json array can only be a hash value not key");
                }
                if (this.tree.isIntermediaryPath(level.jsonPointer())) {
                    this.pushLevel(Mode.JSON_ARRAY);
                    level.removeLastTokenFromJsonPointer();
                    return;
                }
                level.pushMode(Mode.JSON_ARRAY_VALUE);
                readValue(level);
            } else if (this.currentChar == JSON_T) {
                if (!level.isHashValue()) {
                    throw new IllegalStateException("Invalid json, json true can only be a hash value not key");
                }
                level.pushMode(Mode.JSON_BOOLEAN_TRUE_VALUE);
                readValue(level);
            } else if (this.currentChar == JSON_F) {
                if (!level.isHashValue()) {
                    throw new IllegalStateException("Invalid json, json false can only be a hash value not key");
                }
                level.pushMode(Mode.JSON_BOOLEAN_FALSE_VALUE);
                readValue(level);
            } else if (this.currentChar == JSON_N) {
                if (!level.isHashValue()) {
                    throw new IllegalStateException("Invalid json, json null can only be a hash value not key");
                }
                level.pushMode(Mode.JSON_NULL_VALUE);
                readValue(level);
            } else if (isNumber(this.currentChar)) {
                if (!level.isHashValue()) {
                    throw new IllegalStateException("Invalid json, json number can only be a hash value not key");
                }
                level.pushMode(JSON_NUMBER_VALUE);
                readValue(level);
            } else if (this.currentChar == JSON_COMMA) {
                level.isHashValue(false);
            } else if (this.currentChar == C_CURLY) {
                popAndResetToOldLevel();
                return;
            }
        }
    }

    /**
     * Handle the logic for reading down a JSON Array.
     *
     * @param level the current level.
     * @throws EOFException if more data is needed.
     */
    private void readArray(final JsonLevel level) throws EOFException {
        while (true) {
            readNextChar(level);
            if (this.currentChar == JSON_ST) {
                level.pushMode(Mode.JSON_STRING_VALUE);
                readValue(level);
            } else if (this.currentChar == O_CURLY) {
                if (this.tree.isIntermediaryPath(level.jsonPointer())) {
                    this.pushLevel(Mode.JSON_OBJECT);
                    return;
                }
                level.pushMode(Mode.JSON_OBJECT_VALUE);
                readValue(level);
            } else if (this.currentChar == O_SQUARE) {
                if (this.tree.isIntermediaryPath(level.jsonPointer())) {
                    this.pushLevel(Mode.JSON_ARRAY);
                    return;
                } else {
                    level.pushMode(Mode.JSON_ARRAY_VALUE);
                    readValue(level);
                }
            } else if (this.currentChar == JSON_T) {
                level.pushMode(Mode.JSON_BOOLEAN_TRUE_VALUE);
                readValue(level);
            } else if (this.currentChar == JSON_F) {
                level.pushMode(Mode.JSON_BOOLEAN_FALSE_VALUE);
                readValue(level);
            } else if (this.currentChar == JSON_N) {
                level.pushMode(Mode.JSON_NULL_VALUE);
                readValue(level);
            } else if (isNumber(this.currentChar)) {
                level.pushMode(JSON_NUMBER_VALUE);
                readValue(level);
            } else if (this.currentChar == JSON_COMMA) {
                level.updateIndex();
                level.setArrayIndexOnJsonPointer();
            } else if (this.currentChar == C_SQUARE) {
                popAndResetToOldLevel();
                return;
            }
        }
    }

    /**
     * Handle the logic for reading down a JSON Value.
     *
     * @param level the current level.
     * @throws EOFException if more data is needed.
     */
    private void readValue(final JsonLevel level) throws EOFException {
        int readerIndex = content.readerIndex();
        ByteProcessor processor = null;
        Mode mode = level.peekMode();
        switch (mode) {
            case JSON_ARRAY_VALUE:
                arProcessor.reset();
                processor = arProcessor;
                break;
            case JSON_OBJECT_VALUE:
                obProcessor.reset();
                processor = obProcessor;
                break;
            case JSON_STRING_VALUE:
            case JSON_STRING_HASH_KEY:
                processor = stProcessor;
                break;
            case JSON_NULL_VALUE:
                nullProcessor.reset();
                processor = nullProcessor;
                break;
            case JSON_BOOLEAN_TRUE_VALUE:
                processor = trueProcessor;
                break;
            case JSON_BOOLEAN_FALSE_VALUE:
                processor = falseProcessor;
                break;
            case JSON_NUMBER_VALUE:
                processor = numProcessor;
                break;
        }
        int length;
        boolean shouldSaveValue = tree.isTerminalPath(level.jsonPointer()) || mode == Mode.JSON_STRING_HASH_KEY;

        int lastValidIndex = content.forEachByte(processor);
        if (lastValidIndex == -1) {
            if (mode == Mode.JSON_NUMBER_VALUE && content.readableBytes() > 2) {
                length = 1;
                level.setCurrentValue(this.content.copy(readerIndex - 1, length), length);
                //no need to skip here
                this.content.discardReadBytes();
                level.emitJsonPointerValue();
            } else {
                throw NEED_MORE_DATA;
            }
        } else {
            if (mode == Mode.JSON_OBJECT_VALUE ||
                    mode == Mode.JSON_ARRAY_VALUE ||
                    mode == Mode.JSON_STRING_VALUE ||
                    mode == Mode.JSON_STRING_HASH_KEY ||
                    mode == Mode.JSON_NULL_VALUE ||
                    mode == Mode.JSON_BOOLEAN_TRUE_VALUE ||
                    mode == Mode.JSON_BOOLEAN_FALSE_VALUE) {

                length = lastValidIndex - readerIndex + 1;
                if (shouldSaveValue) {
                    level.setCurrentValue(this.content.copy(readerIndex - 1, length + 1), length);
                    level.emitJsonPointerValue();
                }
                this.content.skipBytes(length);
                this.content.discardReadBytes();
            } else {
                //special handling for number as they don't need structural tokens intact
                //and the processor returns only on an unacceptable value rather than a finite state automaton
                length = lastValidIndex - readerIndex;
                if (length > 0) {
                    if (shouldSaveValue) {
                        level.setCurrentValue(this.content.copy(readerIndex - 1, length + 1), length);
                        level.emitJsonPointerValue();
                    }
                    this.content.skipBytes(length);
                    this.content.discardReadBytes();
                } else {
                    length = 1;
                    if (shouldSaveValue) {
                        level.setCurrentValue(this.content.copy(readerIndex - 1, length), length);
                        this.content.discardReadBytes();
                        level.emitJsonPointerValue();
                    }
                }
            }
        }
        if (mode != Mode.JSON_STRING_HASH_KEY) {
            level.removeLastTokenFromJsonPointer();
        }
        level.popMode();
    }

    /**
     * Reads the UTF-8 Byte Order Mark.
     *
     * @throws EOFException if more input is needed.
     */
    private void readBOM() throws EOFException {
        int readerIndex = content.readerIndex();
        int lastBOMIndex = content.forEachByte(bomProcessor);

        if (lastBOMIndex == -1) {
            throw NEED_MORE_DATA;
        }
        if (lastBOMIndex > readerIndex) {
            this.content.skipBytes(lastBOMIndex - readerIndex + 1);
            this.content.discardReadBytes();
        }

        this.levelStack.pop();
    }

    /**
     * Reads the next character into {@link #currentChar}.
     *
     * @param level the current level of nesting.
     * @throws EOFException if more input is needed.
     */
    private void readNextChar(final JsonLevel level) throws EOFException {
        int readerIndex = content.readerIndex();
        int lastWsIndex = content.forEachByte(wsProcessor);

        if (lastWsIndex == -1 && level != null) {
            throw NEED_MORE_DATA;
        }
        if (lastWsIndex > readerIndex) {
            this.content.skipBytes(lastWsIndex - readerIndex);
            this.content.discardReadBytes();
        }

        this.currentChar = this.content.readByte();
    }

    /**
     * JsonLevel can be a nesting level of an json object or json array.
     */
    static class JsonLevel {

        private final Deque<Mode> modes = new ArrayDeque<>();
        private final JsonPointer jsonPointer;

        private ByteBuf currentValue;
        private boolean isHashValue;
        private boolean isArray;
        private int arrayIndex;

        JsonLevel(final Mode mode, final JsonPointer jsonPointer) {
            this.pushMode(mode);
            this.jsonPointer = jsonPointer;
        }

        void isHashValue(boolean isHashValue) {
            this.isHashValue = isHashValue;
        }

        boolean isHashValue() {
            return this.isHashValue;
        }

        void isArray(boolean isArray) {
            this.isArray = isArray;
        }

        void pushMode(Mode mode) {
            this.modes.push(mode);
        }

        Mode peekMode() {
            return this.modes.peek();
        }

        void popMode() {
            this.modes.pop();
        }

        JsonPointer jsonPointer() {
            return this.jsonPointer;
        }

        void updateIndex() {
            this.arrayIndex++;
        }

        void setCurrentValue(ByteBuf value, int length) {
            this.currentValue = value;
            if (peekMode() == Mode.JSON_STRING_HASH_KEY) {
                String withoutQuotes = this.currentValue.toString(
                  this.currentValue.readerIndex() + 1, length - 1, UTF_8);
                this.jsonPointer.addToken(withoutQuotes);
            }
        }

        void setArrayIndexOnJsonPointer() {
            this.jsonPointer.addToken(Integer.toString(this.arrayIndex));
        }

        void removeLastTokenFromJsonPointer() {
            this.jsonPointer.removeLastToken();
        }

        void emitJsonPointerValue() {
            if ((this.isHashValue || this.isArray) && this.jsonPointer.callback() != null) {
                final byte[] bytes;
                try {
                    bytes = new byte[this.currentValue.readableBytes()];
                    this.currentValue.readBytes(bytes);
                } finally {
                    this.currentValue.release();
                }
                this.jsonPointer.callback().accept(bytes);

            } else {
                this.currentValue.release();
            }
        }
    }
}
