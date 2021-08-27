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

package com.couchbase.client.java.manager.eventing;

import com.couchbase.client.java.query.QueryScanConsistency;

import java.time.Duration;
import java.util.List;

public class EventingFunctionSettings {

  private final EventingFunctionProcessingStatus processingStatus;
  private final EventingFunctionDeploymentStatus deploymentStatus;
  private final long cppWorkerThreadCount;
  private final String dcpStreamBoundary;
  private final String description;
  private final String logLevel;
  private final String languageCompatibility;
  private final Duration executionTimeout;
  private final long lcbInstCapacity;
  private final long lcbRetryCount;
  private final Duration lcbTimeout;
  private final QueryScanConsistency queryConsistency;
  private final long numTimerPartitions;
  private final long sockBatchSize;
  private final Duration tickDuration;
  private final long timerContextSize;
  private final String userPrefix;
  private final long bucketCacheSize;
  private final long bucketCacheAge;
  private final long curlMaxAllowedRespSize;
  private final long workerCount;
  private final boolean queryPrepareAll;
  private final List<String> handlerHeaders;
  private final List<String> handlerFooters;
  private final boolean enableAppLogRotation;
  private final String appLogDir;
  private final long appLogMaxSize;
  private final long appLogMaxFiles;
  private final Duration checkpointInterval;

  public static EventingFunctionSettings create() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  private EventingFunctionSettings(Builder builder) {
    this.processingStatus = builder.processingStatus;
    this.deploymentStatus = builder.deploymentStatus;
    this.cppWorkerThreadCount = builder.cppWorkerThreadCount;
    this.dcpStreamBoundary = builder.dcpStreamBoundary;
    this.description = builder.description;
    this.logLevel = builder.logLevel;
    this.languageCompatibility = builder.languageCompatibility;
    this.executionTimeout = builder.executionTimeout;
    this.lcbInstCapacity = builder.lcbInstCapacity;
    this.lcbRetryCount = builder.lcbRetryCount;
    this.lcbTimeout = builder.lcbTimeout;
    this.queryConsistency = builder.queryConsistency;
    this.numTimerPartitions = builder.numTimerPartitions;
    this.sockBatchSize = builder.sockBatchSize;
    this.tickDuration = builder.tickDuration;
    this.timerContextSize = builder.timerContextSize;
    this.userPrefix = builder.userPrefix;
    this.bucketCacheSize = builder.bucketCacheSize;
    this.bucketCacheAge = builder.bucketCacheAge;
    this.curlMaxAllowedRespSize = builder.curlMaxAllowedRespSize;
    this.workerCount = builder.workerCount;
    this.queryPrepareAll = builder.queryPrepareAll;
    this.handlerHeaders = builder.handlerHeaders;
    this.handlerFooters = builder.handlerFooters;
    this.enableAppLogRotation = builder.enableAppLogRotation;
    this.appLogDir = builder.appLogDir;
    this.appLogMaxSize = builder.appLogMaxSize;
    this.appLogMaxFiles = builder.appLogMaxFiles;
    this.checkpointInterval = builder.checkpointInterval;
  }

  public EventingFunctionProcessingStatus processingStatus() {
    return processingStatus;
  }

  public EventingFunctionDeploymentStatus deploymentStatus() {
    return deploymentStatus;
  }

  public long cppWorkerThreadCount() {
    return cppWorkerThreadCount;
  }

  public String dcpStreamBoundary() {
    return dcpStreamBoundary;
  }

  public String description() {
    return description;
  }

  public String logLevel() {
    return logLevel;
  }

  public String languageCompatibility() {
    return languageCompatibility;
  }

  public Duration executionTimeout() {
    return executionTimeout;
  }

  public long lcbInstCapacity() {
    return lcbInstCapacity;
  }

  public long lcbRetryCount() {
    return lcbRetryCount;
  }

  public Duration lcbTimeout() {
    return lcbTimeout;
  }

  public QueryScanConsistency queryConsistency() {
    return queryConsistency;
  }

  public long numTimerPartitions() {
    return numTimerPartitions;
  }

  public long sockBatchSize() {
    return sockBatchSize;
  }

  public Duration tickDuration() {
    return tickDuration;
  }

  public long timerContextSize() {
    return timerContextSize;
  }

  public String userPrefix() {
    return userPrefix;
  }

  public long bucketCacheSize() {
    return bucketCacheSize;
  }

  public long bucketCacheAge() {
    return bucketCacheAge;
  }

  public long curlMaxAllowedRespSize() {
    return curlMaxAllowedRespSize;
  }

  public long workerCount() {
    return workerCount;
  }

  public boolean queryPrepareAll() {
    return queryPrepareAll;
  }

  public List<String> handlerHeaders() {
    return handlerHeaders;
  }

  public List<String> handlerFooters() {
    return handlerFooters;
  }

  public boolean enableAppLogRotation() {
    return enableAppLogRotation;
  }

  public String appLogDir() {
    return appLogDir;
  }

  public long appLogMaxSize() {
    return appLogMaxSize;
  }

  public long appLogMaxFiles() {
    return appLogMaxFiles;
  }

  public Duration checkpointInterval() {
    return checkpointInterval;
  }

  @Override
  public String toString() {
    return "EventingFunctionSettings{" +
      "cppWorkerThreadCount=" + cppWorkerThreadCount +
      ", dcpStreamBoundary='" + dcpStreamBoundary + '\'' +
      ", description='" + description + '\'' +
      ", logLevel='" + logLevel + '\'' +
      ", languageCompatibility='" + languageCompatibility + '\'' +
      ", executionTimeout=" + executionTimeout +
      ", lcbInstCapacity=" + lcbInstCapacity +
      ", lcbRetryCount=" + lcbRetryCount +
      ", lcbTimeout=" + lcbTimeout +
      ", queryConsistency=" + queryConsistency +
      ", numTimerPartitions=" + numTimerPartitions +
      ", sockBatchSize=" + sockBatchSize +
      ", tickDuration=" + tickDuration +
      ", timerContextSize=" + timerContextSize +
      ", userPrefix='" + userPrefix + '\'' +
      ", bucketCacheSize=" + bucketCacheSize +
      ", bucketCacheAge=" + bucketCacheAge +
      ", curlMaxAllowedRespSize=" + curlMaxAllowedRespSize +
      ", workerCount=" + workerCount +
      ", queryPrepareAll=" + queryPrepareAll +
      ", handlerHeaders=" + handlerHeaders +
      ", handlerFooters=" + handlerFooters +
      ", enableAppLogRotation=" + enableAppLogRotation +
      ", appLogDir='" + appLogDir + '\'' +
      ", appLogMaxSize=" + appLogMaxSize +
      ", appLogMaxFiles=" + appLogMaxFiles +
      ", checkpointInterval=" + checkpointInterval +
      '}';
  }

  public static class Builder {

    private EventingFunctionProcessingStatus processingStatus;
    private EventingFunctionDeploymentStatus deploymentStatus;

    private long cppWorkerThreadCount;
    private String dcpStreamBoundary;
    private String description;
    private String logLevel;
    private String languageCompatibility;
    private Duration executionTimeout;
    private long lcbInstCapacity;
    private long lcbRetryCount;
    private Duration lcbTimeout;
    private QueryScanConsistency queryConsistency;
    private long numTimerPartitions;
    private long sockBatchSize;
    private Duration tickDuration;
    private long timerContextSize;
    private String userPrefix;
    private long bucketCacheSize;
    private long bucketCacheAge;
    private long curlMaxAllowedRespSize;
    private long workerCount;
    private boolean queryPrepareAll;
    private List<String> handlerHeaders;
    private List<String> handlerFooters;
    private boolean enableAppLogRotation;
    private String appLogDir;
    private long appLogMaxSize;
    private long appLogMaxFiles;
    private Duration checkpointInterval;

    Builder processingStatus(EventingFunctionProcessingStatus processingStatus) {
      this.processingStatus = processingStatus;
      return this;
    }

    Builder deploymentStatus(EventingFunctionDeploymentStatus deploymentStatus) {
      this.deploymentStatus = deploymentStatus;
      return this;
    }

    public Builder checkpointInterval(Duration checkpointInterval) {
      this.checkpointInterval = checkpointInterval;
      return this;
    }

    public Builder appLogMaxFiles(long appLogMaxFiles) {
      this.appLogMaxFiles = appLogMaxFiles;
      return this;
    }

    public Builder appLogMaxSize(long appLogMaxSize) {
      this.appLogMaxSize = appLogMaxSize;
      return this;
    }

    public Builder appLogDir(String appLogDir) {
      this.appLogDir = appLogDir;
      return this;
    }

    public Builder enableAppLogRotation(boolean enableAppLogRotation) {
      this.enableAppLogRotation = enableAppLogRotation;
      return this;
    }

    public Builder handlerFooters(List<String> handlerFooters) {
      this.handlerFooters = handlerFooters;
      return this;
    }

    public Builder handlerHeaders(List<String> handlerHeaders) {
      this.handlerHeaders = handlerHeaders;
      return this;
    }

    public Builder queryPrepareAll(boolean queryPrepareAll) {
      this.queryPrepareAll = queryPrepareAll;
      return this;
    }

    public Builder workerCount(long workerCount) {
      this.workerCount = workerCount;
      return this;
    }

    public Builder cppWorkerThreadCount(long cppWorkerThreadCount) {
      this.cppWorkerThreadCount = cppWorkerThreadCount;
      return this;
    }

    public Builder dcpStreamBoundary(String dcpStreamBoundary) {
      this.dcpStreamBoundary = dcpStreamBoundary;
      return this;
    }

    public Builder description(String description) {
      this.description = description;
      return this;
    }

    public Builder logLevel(String logLevel) {
      this.logLevel = logLevel;
      return this;
    }

    public Builder languageCompatibility(String languageCompatibility) {
      this.languageCompatibility = languageCompatibility;
      return this;
    }

    public Builder executionTimeout(Duration executionTimeout) {
      this.executionTimeout = executionTimeout;
      return this;
    }

    public Builder lcbInstCapacity(long lcbInstCapacity) {
      this.lcbInstCapacity = lcbInstCapacity;
      return this;
    }

    public Builder lcbRetryCount(long lcbRetryCount) {
      this.lcbRetryCount = lcbRetryCount;
      return this;
    }

    public Builder lcbTimeout(Duration lcbTimeout) {
      this.lcbTimeout = lcbTimeout;
      return this;
    }

    public Builder queryConsistency(QueryScanConsistency queryConsistency) {
      this.queryConsistency = queryConsistency;
      return this;
    }

    public Builder numTimerPartitions(long numTimerPartitions) {
      this.numTimerPartitions = numTimerPartitions;
      return this;
    }

    public Builder sockBatchSize(long sockBatchSize) {
      this.sockBatchSize = sockBatchSize;
      return this;
    }

    public Builder tickDuration(Duration tickDuration) {
      this.tickDuration = tickDuration;
      return this;
    }

    public Builder timerContextSize(long timerContextSize) {
      this.timerContextSize = timerContextSize;
      return this;
    }

    public Builder userPrefix(String userPrefix) {
      this.userPrefix = userPrefix;
      return this;
    }

    public Builder bucketCacheSize(long bucketCacheSize) {
      this.bucketCacheSize = bucketCacheSize;
      return this;
    }

    public Builder bucketCacheAge(long bucketCacheAge) {
      this.bucketCacheAge = bucketCacheAge;
      return this;
    }

    public Builder curlMaxAllowedRespSize(long curlMaxAllowedRespSize) {
      this.curlMaxAllowedRespSize = curlMaxAllowedRespSize;
      return this;
    }

    public EventingFunctionSettings build() {
      return new EventingFunctionSettings(this);
    }
  }
}
