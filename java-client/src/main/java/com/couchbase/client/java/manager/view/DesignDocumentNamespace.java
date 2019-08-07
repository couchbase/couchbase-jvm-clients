/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.client.java.manager.view;

import com.couchbase.client.core.annotation.Stability;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.CbStrings.removeStart;

@Stability.Volatile
public enum DesignDocumentNamespace {

  DEVELOPMENT() {
    @Override
    String adjustName(String name) {
      return name.startsWith(DEV_PREFIX) ? name : DEV_PREFIX + name;
    }

    @Override
    boolean contains(String rawDesignDocName) {
      return rawDesignDocName.startsWith(DEV_PREFIX);
    }
  },

  PRODUCTION {
    @Override
    String adjustName(String name) {
      return removeStart(name, DEV_PREFIX);
    }

    @Override
    boolean contains(String rawDesignDocName) {
      return !DEVELOPMENT.contains(rawDesignDocName);
    }
  };

  static final String DEV_PREFIX = "dev_";

  static String requireUnqualified(String name) {
    if (name.startsWith(DEV_PREFIX)) {
      throw new IllegalArgumentException(
          "Design document name '" + redactMeta(name) + "' must not start with '" + DEV_PREFIX + "'" +
              "; instead specify the " + DEVELOPMENT.name() + " namespace when referring to the document.");
    }
    return name;
  }

  abstract String adjustName(String name);

  abstract boolean contains(String rawDesignDocName);
}
