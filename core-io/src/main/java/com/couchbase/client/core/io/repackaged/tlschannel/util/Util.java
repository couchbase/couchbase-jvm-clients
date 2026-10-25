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
 *
 * Logs with SLF4J instead of java.util.logging.
 */

package com.couchbase.client.core.io.repackaged.tlschannel.util;

import javax.net.ssl.SSLEngineResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {

    private static final Logger logger = LoggerFactory.getLogger(Util.class);

    public static void assertTrue(boolean condition) {
        if (!condition) throw new AssertionError();
    }

    /**
     * Convert a {@link SSLEngineResult} into a {@link String}; this is needed because the supplied
     * method includes a log-breaking newline.
     *
     * @param result the SSLEngineResult
     * @return the resulting string
     */
    public static String resultToString(SSLEngineResult result) {
        return String.format(
                "status=%s,handshakeStatus=%s,bytesProduced=%d,bytesConsumed=%d",
                result.getStatus(), result.getHandshakeStatus(), result.bytesProduced(), result.bytesConsumed());
    }

    public static int getJavaMajorVersion() {
        String version = System.getProperty("java.version");
        if (version.startsWith("1.")) {
            version = version.substring(2);
        }
        // Allow these formats:
        // 1.8.0_72-ea
        // 9-ea
        // 9
        // 9.0.1
        // 17
        int dotPos = version.indexOf('.');
        int dashPos = version.indexOf('-');
        return Integer.parseInt(version.substring(0, dotPos > -1 ? dotPos : dashPos > -1 ? dashPos : version.length()));
    }
}
