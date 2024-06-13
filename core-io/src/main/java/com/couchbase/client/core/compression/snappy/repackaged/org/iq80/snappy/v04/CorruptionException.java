/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * THIS FILE HAS BEEN MODIFIED FROM THE ORIGINAL VERSION.
 *
 * The original came from https://github.com/dain/snappy/ at tag "snappy-0.4".
 *
 * It was relocated into a different package to avoid conflicts with
 * the original.
 */
package com.couchbase.client.core.compression.snappy.repackaged.org.iq80.snappy.v04;

import com.couchbase.client.core.annotation.Stability;

@Stability.Internal
public class CorruptionException
        extends RuntimeException
{
    public CorruptionException()
    {
    }

    public CorruptionException(String message)
    {
        super(message);
    }

    public CorruptionException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public CorruptionException(Throwable cause)
    {
        super(cause);
    }
}
