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

import javax.net.ssl.SSLException;

/**
 * Thrown during {@link TlsChannel} handshake to indicate that a user-supplied function threw an
 * exception.
 */
public class TlsChannelCallbackException extends SSLException {
    private static final long serialVersionUID = 8491908031320425318L;

    /**
     * Creates an instance of this class.
     *
     * @param message a human-readable message hinting the place of the error
     * @param throwable the original exception
     */
    public TlsChannelCallbackException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
