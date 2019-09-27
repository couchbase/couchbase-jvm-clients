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

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.view.DesignDocumentNamespace;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static java.util.Objects.requireNonNull;

public class DesignDocumentNotFoundException extends CouchbaseException {
  private final String name;
  private final DesignDocumentNamespace namespace;

  private DesignDocumentNotFoundException(String name, DesignDocumentNamespace namespace) {
    super("Design document [" + redactMeta(name) + "] not found in namespace " + namespace + ".");
    this.name = requireNonNull(name);
    this.namespace = requireNonNull(namespace);
  }

  public static DesignDocumentNotFoundException forName(String name, DesignDocumentNamespace namespace) {
    return new DesignDocumentNotFoundException(name, namespace);
  }

  public String name() {
    return name;
  }

  public DesignDocumentNamespace namespace() {
    return namespace;
  }
}
