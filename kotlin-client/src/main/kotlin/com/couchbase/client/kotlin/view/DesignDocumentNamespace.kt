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

package com.couchbase.client.kotlin.view

import com.couchbase.client.core.logging.RedactableArgument.redactMeta

public enum class DesignDocumentNamespace {
    DEVELOPMENT {
        override fun adjustName(name: String): String =
            if (name.startsWith(DEV_PREFIX)) name else DEV_PREFIX + name

        override operator fun contains(rawDesignDocName: String): Boolean =
            rawDesignDocName.startsWith(DEV_PREFIX)

    },
    PRODUCTION {
        override fun adjustName(name: String): String = name.removePrefix(DEV_PREFIX)

        override operator fun contains(rawDesignDocName: String): Boolean {
            return rawDesignDocName !in DEVELOPMENT
        }
    },
    ;

    internal abstract fun adjustName(name: String): String
    internal abstract operator fun contains(rawDesignDocName: String): Boolean

    internal companion object {
        const val DEV_PREFIX = "dev_"

        fun requireUnqualified(name: String): String {
            require(!name.startsWith(DEV_PREFIX)) {
                "Design document name '${redactMeta(name)}' must not start with '$DEV_PREFIX';" +
                        " instead specify the ${DEVELOPMENT.name} namespace when referring to the document."
            }
            return name
        }
    }
}

