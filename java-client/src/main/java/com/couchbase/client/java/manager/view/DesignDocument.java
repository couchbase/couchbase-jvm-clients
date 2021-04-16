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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.manager.CoreViewIndexManager.requireUnqualifiedName;

/**
 * A collection of named {@link View}s.
 */
@Stability.Volatile
public class DesignDocument {

  private String name;
  private Map<String, View> views;

  /**
   * Creates a new design document with the given name and no views.
   *
   * @param name name for the design document
   */
  public DesignDocument(String name) {
    this(name, Collections.emptyMap());
  }

  /**
   * Creates a new design document with the given name and views.
   *
   * @param name name for the design document
   * @param views map from view name to view
   */
  public DesignDocument(String name, Map<String, View> views) {
    name(name);
    views(views);
  }

  /**
   * Returns the name of the design document.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the views in this document indexed by name. Changes to the returned map affect the document,
   * so for example you can remove all views by calling {@code myDesignDoc.views().clear()}.
   */
  public Map<String, View> views() {
    return views;
  }

  /**
   * Convenience method for adding a view to this design document (replacing any existing view with the same name).
   */
  public DesignDocument putView(String name, String map) {
    return putView(name, map, null);
  }

  /**
   * Convenience method for adding a view to this design document (replacing any existing view with the same name).
   */
  public DesignDocument putView(String name, String map, String reduce) {
    return putView(name, new View(map, reduce));
  }

  /**
   * Convenience method for adding a view to this design document (replacing any existing view with the same name).
   */
  public DesignDocument putView(String name, View view) {
    views.put(name, view);
    return this;
  }

  /**
   * Returns the view with the given name, if it exists.
   *
   * @param name name of the view to return
   */
  public Optional<View> getView(String name) {
    return Optional.ofNullable(views.get(name));
  }

  /**
   * Removes the view with the give name, if it exists.
   *
   * @param name name of the view to remove
   * @return the design document, for method chaining.
   */
  public DesignDocument removeView(String name) {
    views.remove(name);
    return this;
  }

  /**
   * Assigns a new name to this design document.
   *
   * @param name new name for the design document.
   * @return the design document, for method chaining.
   */
  public DesignDocument name(String name) {
    this.name = requireUnqualifiedName(name);
    return this;
  }

  /**
   * Sets all of the views for this design document, overriding any existing views.
   *
   * @param views map from view name to view
   * @return the design document, for method chaining.
   */
  public DesignDocument views(Map<String, View> views) {
    this.views = new HashMap<>(views);
    return this;
  }

  @Override
  public String toString() {
    return "DesignDocument{" +
        "name='" + redactMeta(name) + '\'' +
        ", views=" + redactMeta(views) +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DesignDocument that = (DesignDocument) o;
    return name.equals(that.name) &&
        views.equals(that.views);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
