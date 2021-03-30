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


/**
 * Describes how the outer document store semantics on subdoc should act.
 */
public sealed class StoreSemantics {
    public class Replace internal constructor(
        public val cas: Long,
        public val createParent: Boolean,
    ) : StoreSemantics()

    public object Upsert : StoreSemantics()
    public object Insert : StoreSemantics()

    public companion object {
        private val ReplaceNoCasCreateParent = Replace(0, true)
        private val ReplaceNoCasNoCreateParent = Replace(0, false)

        /**
         * Replace an existing document. Fail if the document does not exist.
         * Fail if a CAS value is specified and it does not match the existing
         * document's CAS value.
         *
         * @param cas If non-zero, throw CasMismatchException if the existing
         * document's CAS value does not match this value.
         * @param createParent If false, all mutations throw PathNotFoundException
         * if the target's parent node does not exist. If true, the
         * following mutations will create any missing parent nodes:
         * * `insert`
         * * `upsert`
         * * `arrayAppend`
         * * `arrayPrepend`
         * * `arrayAddUnique`
         * * `incrementAndGet`
         * * `decrementAndGet`
         */
        public fun replace(cas: Long = 0, createParent: Boolean = true): StoreSemantics =
            when (cas) {
                0L -> if (createParent) ReplaceNoCasCreateParent else ReplaceNoCasNoCreateParent
                else -> Replace(cas, createParent)
            }

        /**
         * Replace the document, or create it if it does not exist.
         */
        public fun upsert(): StoreSemantics = Upsert

        /**
         * Create the document; fail if it already exists.
         */
        public fun insert(): StoreSemantics = Insert
    }
}
