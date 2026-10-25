/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * These classes came from https://github.com/marianobarrios/tls-channel/ at tag "1.0.0" (MIT License; each file
 * has the license notice). Only the client side was copied: not the server side, the asynchronous channels,
 * or the direct buffer allocator.
 * <p>
 * They have been modified so a failed write doesn't stop reads (see {@code TlsChannelImpl}),
 * so the TLS alert that made the server close the connection can still be read.
 * They also log with SLF4J instead of java.util.logging, and {@code BufferHolder.zero()} no longer
 * throws {@link java.nio.InvalidMarkException} when the buffer holds data.
 * <p>
 * Used by the non-blocking KV connections, for TLS over a {@link java.nio.channels.SocketChannel}.
 */
package com.couchbase.client.core.io.repackaged.tlschannel;
