/*
 * Copyright (c) 2018 Couchbase, Inc.
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
package com.couchbase.client.core.cnc;

import com.couchbase.client.core.json.Mapper;

import java.util.Map;

/**
 * Common parent method for all contexts.
 *
 * Contexts are encouraged to derive from this abstract class because all they have
 * to do then is to implement/override {@link #injectExportableParams()} and feed
 * the data they want to be extracted. The actual extraction and formatting then
 * comes for free.
 */
public abstract class AbstractContext implements Context {

    /**
     * This method needs to be implemented by the actual context implementations to
     * inject the params they need for exporting.
     *
     * @return return the data or null.
     */
    protected Map<String, Object> injectExportableParams() {
        return null;
    }

    @Override
    public String exportAsString(final ExportFormat format) {
        Map<String, Object> input = injectExportableParams();
        if (input != null) {
            switch (format) {
                case JSON:
                    return Mapper.encodeAsString(input);
                case JSON_PRETTY:
                    return Mapper.encodeAsStringPretty(input);
                default:
                    throw new UnsupportedOperationException("Unsupported ExportFormat " + format);
            }
        }
        return null;
    }
}
