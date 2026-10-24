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

/**
 * This exception signals the caller that the operation could not continue because a CPU-intensive
 * operation (typically a TLS handshake) needs to be executed and the {@link TlsChannel} is
 * configured to not run tasks. This allows the application to run these tasks in other
 * threads, in order to not slow the selection loop. The method that threw the exception should be
 * retried once the task supplied by {@link #getTask()} is executed and finished.
 *
 * <p>This exception is akin to the SSL_ERROR_WANT_ASYNC error code used by OpenSSL (but note that
 * in OpenSSL, the task is executed by the library, while with the {@link TlsChannel}, the calling
 * code is responsible for the execution).
 *
 * @see <a href="https://www.openssl.org/docs/man1.1.0/ssl/SSL_get_error.html">OpenSSL error
 *     documentation</a>
 */
public class NeedsTaskException extends TlsChannelFlowControlException {

    private static final long serialVersionUID = -936451835836926915L;
    private final transient Runnable task;

    /**
     * Creates this class registering the supplied {@link Runnable} as the needed task.
     *
     * @param task the needed task
     */
    public NeedsTaskException(Runnable task) {
        this.task = task;
    }

    public Runnable getTask() {
        return task;
    }
}
