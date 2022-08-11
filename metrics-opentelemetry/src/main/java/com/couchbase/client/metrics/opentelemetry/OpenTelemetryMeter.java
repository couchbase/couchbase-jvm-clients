/*
 * Copyright (c) 2020 Couchbase, Inc.
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

package com.couchbase.client.metrics.opentelemetry;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.Counter;
import com.couchbase.client.core.cnc.Meter;
import com.couchbase.client.core.cnc.ValueRecorder;
import com.couchbase.client.core.cnc.metrics.NameAndTags;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.MeterException;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.MeterProvider;

import java.net.URL;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

@Stability.Volatile
public class OpenTelemetryMeter implements Meter {

  public static final String INSTRUMENTATION_NAME = "com.couchbase.client.jvm";

  private static final Map<String, Attributes> MANIFEST_INFOS = new ConcurrentHashMap<>();

  static {
    try {
      Enumeration<URL> resources = CoreEnvironment.class.getClassLoader().getResources(JarFile.MANIFEST_NAME);
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
          if (entry.getKey().startsWith("couchbase-")) {
            MANIFEST_INFOS.put(entry.getKey(), entry.getValue());
          }
        }
      }
    } catch (Exception e) {
      // Ignored on purpose.
    }
  }

  private final io.opentelemetry.api.metrics.Meter otMeter;
  private final Map<NameAndTags, OpenTelemetryCounter> counters = new ConcurrentHashMap<>();
  private final Map<NameAndTags, OpenTelemetryValueRecorder> valueRecorders = new ConcurrentHashMap<>();

  @Stability.Volatile
  public static OpenTelemetryMeter wrapGlobal() {
    return wrap(GlobalOpenTelemetry.get());
  }
  
  @Stability.Volatile
  public static OpenTelemetryMeter wrap(final OpenTelemetry openTelemetry) {
    return new OpenTelemetryMeter(openTelemetry.getMeterProvider());
  }

  @Stability.Volatile
  public static OpenTelemetryMeter wrap(final MeterProvider meterProvider) {
    return new OpenTelemetryMeter(meterProvider);
  }

  private OpenTelemetryMeter(MeterProvider meterProvider) {
    String version = null;
    try {
      version = MANIFEST_INFOS.get("couchbase-java-metrics-opentelemetry").getValue("Impl-Version");
    } catch (Exception ex) {
      // ignored on purpose
    }

    this.otMeter = version != null
      ? meterProvider.meterBuilder(INSTRUMENTATION_NAME).setInstrumentationVersion(version).build()
      : meterProvider.get(INSTRUMENTATION_NAME);
  }

  @Override
  public Counter counter(final String name, final Map<String, String> tags) {
    try {
      return counters.computeIfAbsent(new NameAndTags(name, tags), key -> {
        LongCounter counter = otMeter.counterBuilder(name).build();
        AttributesBuilder builder = io.opentelemetry.api.common.Attributes.builder();
        for (Map.Entry<String, String> tag : tags.entrySet()) {
          builder.put(tag.getKey(), tag.getValue());
        }
        return new OpenTelemetryCounter(counter, builder.build());
      });
    } catch (Exception ex) {
      throw new MeterException("Failed to create/access Counter", ex);
    }
  }

  @Override
  public ValueRecorder valueRecorder(final String name, final Map<String, String> tags) {
    try {
      return valueRecorders.computeIfAbsent(new NameAndTags(name, tags), key -> {
        DoubleHistogram vc =  otMeter.histogramBuilder(name).build();
        AttributesBuilder builder = io.opentelemetry.api.common.Attributes.builder();
        for (Map.Entry<String, String> tag : tags.entrySet()) {
          builder.put(tag.getKey(), tag.getValue());
        }
        return new OpenTelemetryValueRecorder(vc, builder.build());
      });
    } catch (Exception ex) {
      throw new MeterException("Failed to create/access ValueRecorder", ex);
    }
  }

}
