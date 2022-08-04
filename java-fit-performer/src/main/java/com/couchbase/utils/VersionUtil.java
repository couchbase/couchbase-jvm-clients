/*
 * Copyright 2022 Couchbase, Inc.
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
package com.couchbase.utils;

import com.couchbase.InternalPerformerFailure;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

public class VersionUtil {
    private VersionUtil() {
    }

    public static
    @Nullable String introspectSDKVersion() {
        try {
            Enumeration<URL> resources = VersionUtil.class.getClassLoader().getResources(JarFile.MANIFEST_NAME);
            while (resources.hasMoreElements()) {
                URL manifestUrl = resources.nextElement();
                if (manifestUrl == null) {
                    continue;
                }
                Manifest manifest = new Manifest(manifestUrl.openStream());

                if (manifest.getEntries() == null) {
                    continue;
                }
                for (Map.Entry<String, Attributes> entry : manifest.getEntries().entrySet()) {
                    System.out.println(entry.getKey());
                    if (entry.getKey().equals("couchbase-java")) {
                        return entry.getValue().getValue("Impl-Version");
                    }
                }
            }
            // TODO ESI throw new InternalPerformerFailure(new RuntimeException("Unable to introspect library version"));
            return "3.3.0";
        } catch (IOException err) {
            // Sometimes see "NoSuchFileException: /proc/136/fd/4" on performance runs
            return null;
        }
    }
}
