/*
 * Copyright (c) 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.scala.view

import com.couchbase.client.core.logging.RedactableArgument.redactMeta

import scala.util.{Failure, Success, Try}

/** Specifies what namespace the design document is in, e.g. production or development.
  */
sealed trait DesignDocumentNamespace {

  private[scala] def adjustName(name: String): String
  private[scala] def contains(rawDesignDocName: String): Boolean
}

object DesignDocumentNamespace {
  case object Development extends DesignDocumentNamespace {
    private[scala] override def adjustName(name: String): String = {
      if (name.startsWith(DevPrefix)) name else DevPrefix + name
    }

    private[scala] override def contains(rawDesignDocName: String): Boolean = {
      rawDesignDocName.startsWith(DevPrefix)
    }
  }

  case object Production extends DesignDocumentNamespace {
    private[scala] override def adjustName(name: String): String = {
      name.stripPrefix(DevPrefix)
    }

    private[scala] override def contains(rawDesignDocName: String): Boolean = {
      !rawDesignDocName.startsWith(DevPrefix)
    }
  }

  private val DevPrefix = "dev_"

  private[scala] def requireUnqualified(name: String): Try[String] = {
    if (name.startsWith(DevPrefix)) {
      Failure(
        new IllegalArgumentException(
          "Design document name '" + redactMeta(name) + "' must not start with '" +
            DevPrefix + "'" + "; instead specify the Development namespace when referring to the document."
        )
      )
    } else Success(name)
  }
}
