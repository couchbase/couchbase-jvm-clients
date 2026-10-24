/*
 * MIT License
 *
 * Copyright (c) all contributors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

/*
 * THIS FILE HAS BEEN MODIFIED FROM THE ORIGINAL VERSION.
 *
 * The original came from https://github.com/marianobarrios/tls-channel/ at tag "1.0.0".
 *
 * It was relocated into a different package to avoid conflicts with
 * the original.
 */

package com.couchbase.client.core.io.repackaged.tlschannel;

import java.nio.ByteBuffer;

/**
 * Allocator that creates heap buffers. The {@link #free(ByteBuffer)} method is a no-op, as heap
 * buffers are handled completely by the garbage collector.
 *
 * <p>Heap buffers are generally used as a simple and generally good enough default solution.
 */
public class HeapBufferAllocator implements BufferAllocator {

    @Override
    public ByteBuffer allocate(int size) {
        return ByteBuffer.allocate(size);
    }

    @Override
    public void free(ByteBuffer buffer) {
        // GC does it
    }
}
