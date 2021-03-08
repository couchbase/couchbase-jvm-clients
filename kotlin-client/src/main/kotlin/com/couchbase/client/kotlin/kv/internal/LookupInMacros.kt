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

package com.couchbase.client.kotlin.kv.internal

internal class LookupInMacro {
    internal companion object {
        internal const val DOCUMENT = "\$document"

        internal const val EXPIRY_TIME = "\$document.exptime"

        internal const val CAS = "\$document.CAS"

        internal const val SEQ_NO = "\$document.seqno"

        internal const val LAST_MODIFIED = "\$document.last_modified"

        internal const val IS_DELETED = "\$document.deleted"

        internal const val VALUE_SIZE_BYTES = "\$document.value_bytes"

        internal const val REV_ID = "\$document.revid"

        internal const val FLAGS = "\$document.flags"
    }
}
