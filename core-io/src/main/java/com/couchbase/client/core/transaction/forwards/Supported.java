/*
 * Copyright 2022 Couchbase, Inc.
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
package com.couchbase.client.core.transaction.forwards;

import com.couchbase.client.core.annotation.Stability;

import java.util.Set;

/**
 * Defines what is support by this implementation (extensions and protocol version).
 */
@Stability.Internal
public class Supported {
    public final Set<Extension> extensions = Extension.SUPPORTED;
    public final int protocolMajor = 2;
    public final int protocolMinor = 1;

    public final static Supported SUPPORTED = new Supported();

    @Override
    public String toString() {
        return "Supported {extensions=" + extensions.toString() + ", protocol=" + protocolMajor + "." + protocolMinor + "}";
    }
}
