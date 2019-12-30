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

package com.couchbase.client.java.view;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.InvalidArgumentException;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.CbStrings.removeStart;

@Stability.Volatile
public enum DesignDocumentNamespace {

  DEVELOPMENT() {
    @Override
    public String adjustName(String name) {
      return name.startsWith(DEV_PREFIX) ? name : DEV_PREFIX + name;
    }

    @Override
    public boolean contains(String rawDesignDocName) {
      return rawDesignDocName.startsWith(DEV_PREFIX);
    }
  },

  PRODUCTION {
    @Override
    public String adjustName(String name) {
      return removeStart(name, DEV_PREFIX);
    }

    @Override
    public boolean contains(String rawDesignDocName) {
      return !DEVELOPMENT.contains(rawDesignDocName);
    }
  };

  static final String DEV_PREFIX = "dev_";

  public static String requireUnqualified(final String name) {
    if (name.startsWith(DEV_PREFIX)) {
      throw InvalidArgumentException.fromMessage(
          "Design document name '" + redactMeta(name) + "' must not start with '" + DEV_PREFIX + "'" +
              "; instead specify the " + DEVELOPMENT.name() + " namespace when referring to the document.");
    }
    return name;
  }

  public abstract String adjustName(String name);

  public abstract boolean contains(String rawDesignDocName);
}
