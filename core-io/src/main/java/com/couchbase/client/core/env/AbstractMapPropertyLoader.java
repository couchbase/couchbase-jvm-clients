/*
 * Copyright (c) 2021 Couchbase, Inc.
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

package com.couchbase.client.core.env;

import java.util.Map;

/**
 * Loads properties from a string/string map.
 *
 * @param <B> the builder to load into.
 */
public abstract class AbstractMapPropertyLoader<B extends CoreEnvironment.Builder> implements PropertyLoader<B> {

  /**
   * Holds the dynamic setter to build the properties.
   */
  private final BuilderPropertySetter setter = new BuilderPropertySetter();

  protected AbstractMapPropertyLoader() {}

  /**
   * Returns the property map which should be loaded.
   *
   * @return the property map to load.
   */
  protected abstract Map<String, String> propertyMap();

  @Override
  public void load(B builder) {
    setter.set(builder, propertyMap());
  }

}
