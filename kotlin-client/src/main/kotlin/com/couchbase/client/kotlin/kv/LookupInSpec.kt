/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin.kv

import com.couchbase.client.core.msg.kv.SubdocCommandType
import com.couchbase.client.core.msg.kv.SubdocGetRequest
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.kv.internal.LookupInMacro

public class Subdoc internal constructor(
    public val path: String,
    public val xattr: Boolean,
    internal val spec: LookupInSpec,
    internal val index: Int,
) {
    public fun contentAsBytes(result: LookupInResult): ByteArray = with(result) { contentAsBytes }
    public inline fun <reified T> contentAs(result: LookupInResult): T = with(result) { contentAs() }

    override fun toString(): String {
        return "Subdoc(path='$path', xattr=$xattr)"
    }
}

public class SubdocCount internal constructor(
    public val path: String,
    public val xattr: Boolean,
    internal val spec: LookupInSpec,
    internal val index: Int,
) {
    public fun get(result: LookupInResult): Int = with(result) { value }

    override fun toString(): String {
        return "SubdocCount(path='$path', xattr=$xattr)"
    }
}

public class SubdocExists internal constructor(
    public val path: String,
    public val xattr: Boolean,
    internal val spec: LookupInSpec,
    internal val index: Int,
) {
    public fun get(result: LookupInResult): Boolean = with(result) { value }

    override fun toString(): String {
        return "SubdocExists(path='$path', xattr=$xattr)"
    }
}

/**
 * Specifies which fields to retrieve when doing a subdoc lookup.
 *
 * @sample com.couchbase.client.kotlin.samples.subdocLookup
 */
public abstract class LookupInSpec {
    internal val commands = ArrayList<SubdocGetRequest.Command>()

    protected fun get(path: String, xattr: Boolean = false): Subdoc {
        val origIndex = commands.size
        val subdoc = Subdoc(path, xattr, this, origIndex)
        val type = if (path == "") SubdocCommandType.GET_DOC else SubdocCommandType.GET
        commands.add(SubdocGetRequest.Command(type, path, xattr, origIndex))
        return subdoc
    }

    @VolatileCouchbaseApi
    protected fun get(macro: LookupInMacro): Subdoc = get(macro.value, xattr = true)

    protected fun count(path: String, xattr: Boolean = false): SubdocCount {
        val origIndex = commands.size
        val subdoc = SubdocCount(path, xattr, this, origIndex)
        commands.add(SubdocGetRequest.Command(SubdocCommandType.COUNT, path, xattr, origIndex))
        return subdoc
    }

    protected fun exists(path: String, xattr: Boolean = false): SubdocExists {
        val origIndex = commands.size
        val subdoc = SubdocExists(path, xattr, this, origIndex)
        commands.add(SubdocGetRequest.Command(SubdocCommandType.EXISTS, path, xattr, origIndex))
        return subdoc
    }

    override fun toString(): String {
        return "LookupInSpec(items=$commands)"
    }
}
