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

  public EventingFunctionProcessingStatus processingStatus() {
    return processingStatus;
  }

  public EventingFunctionSettings processingStatus(EventingFunctionProcessingStatus processingStatus) {
    this.processingStatus = processingStatus;
    return this;
  }

  public EventingFunctionDeploymentStatus deploymentStatus() {
    return deploymentStatus;
  }

  public EventingFunctionSettings deploymentStatus(EventingFunctionDeploymentStatus deploymentStatus) {
    this.deploymentStatus = deploymentStatus;
    return this;
  }

  public long cppWorkerThreadCount() {
    return cppWorkerThreadCount;
  }

  public EventingFunctionSettings cppWorkerThreadCount(long cppWorkerThreadCount) {
    this.cppWorkerThreadCount = cppWorkerThreadCount;
    return this;
  }

  public String dcpStreamBoundary() {
    return dcpStreamBoundary;
  }

  public EventingFunctionSettings dcpStreamBoundary(String dcpStreamBoundary) {
    this.dcpStreamBoundary = dcpStreamBoundary;
    return this;
  }

  public String description() {
    return description;
  }

  public EventingFunctionSettings description(String description) {
    this.description = description;
    return this;
  }

  public String logLevel() {
    return logLevel;
  }

  public EventingFunctionSettings logLevel(String logLevel) {
    this.logLevel = logLevel;
    return this;
  }

  public String languageCompatibility() {
    return languageCompatibility;
  }

  public EventingFunctionSettings languageCompatibility(String languageCompatibility) {
    this.languageCompatibility = languageCompatibility;
    return this;
  }

  public Duration executionTimeout() {
    return executionTimeout;
  }

  public EventingFunctionSettings executionTimeout(Duration executionTimeout) {
    this.executionTimeout = executionTimeout;
    return this;
  }

  public long lcbInstCapacity() {
    return lcbInstCapacity;
  }

  public EventingFunctionSettings lcbInstCapacity(long lcbInstCapacity) {
    this.lcbInstCapacity = lcbInstCapacity;
    return this;
  }

  public long lcbRetryCount() {
    return lcbRetryCount;
  }

  public EventingFunctionSettings lcbRetryCount(long lcbRetryCount) {
    this.lcbRetryCount = lcbRetryCount;
    return this;
  }

  public Duration lcbTimeout() {
    return lcbTimeout;
  }

  public EventingFunctionSettings lcbTimeout(Duration lcbTimeout) {
    this.lcbTimeout = lcbTimeout;
    return this;
  }

  public QueryScanConsistency queryConsistency() {
    return queryConsistency;
  }

  public EventingFunctionSettings queryConsistency(QueryScanConsistency queryConsistency) {
    this.queryConsistency = queryConsistency;
    return this;
  }

  public long numTimerPartitions() {
    return numTimerPartitions;
  }

  public EventingFunctionSettings numTimerPartitions(long numTimerPartitions) {
    this.numTimerPartitions = numTimerPartitions;
    return this;
  }

  public long sockBatchSize() {
    return sockBatchSize;
  }

  public EventingFunctionSettings sockBatchSize(long sockBatchSize) {
    this.sockBatchSize = sockBatchSize;
    return this;
  }

  public Duration tickDuration() {
    return tickDuration;
  }

  public EventingFunctionSettings tickDuration(Duration tickDuration) {
    this.tickDuration = tickDuration;
    return this;
  }

  public long timerContextSize() {
    return timerContextSize;
  }

  public EventingFunctionSettings timerContextSize(long timerContextSize) {
    this.timerContextSize = timerContextSize;
    return this;
  }

  public String userPrefix() {
    return userPrefix;
  }

  public EventingFunctionSettings userPrefix(String userPrefix) {
    this.userPrefix = userPrefix;
    return this;
  }

  public long bucketCacheSize() {
    return bucketCacheSize;
  }

  public EventingFunctionSettings bucketCacheSize(long bucketCacheSize) {
    this.bucketCacheSize = bucketCacheSize;
    return this;
  }

  public long bucketCacheAge() {
    return bucketCacheAge;
  }

  public EventingFunctionSettings bucketCacheAge(long bucketCacheAge) {
    this.bucketCacheAge = bucketCacheAge;
    return this;
  }

  public long curlMaxAllowedRespSize() {
    return curlMaxAllowedRespSize;
  }

  public EventingFunctionSettings curlMaxAllowedRespSize(long curlMaxAllowedRespSize) {
    this.curlMaxAllowedRespSize = curlMaxAllowedRespSize;
    return this;
  }

  public long workerCount() {
    return workerCount;
  }

  public EventingFunctionSettings workerCount(long workerCount) {
    this.workerCount = workerCount;
    return this;
  }

  public boolean queryPrepareAll() {
    return queryPrepareAll;
  }

  public EventingFunctionSettings queryPrepareAll(boolean queryPrepareAll) {
    this.queryPrepareAll = queryPrepareAll;
    return this;
  }

  public List<String> handlerHeaders() {
    return handlerHeaders;
  }

  public EventingFunctionSettings handlerHeaders(List<String> handlerHeaders) {
    this.handlerHeaders = handlerHeaders;
    return this;
  }

  public List<String> handlerFooters() {
    return handlerFooters;
  }

  public EventingFunctionSettings handlerFooters(List<String> handlerFooters) {
    this.handlerFooters = handlerFooters;
    return this;
  }

  public boolean enableAppLogRotation() {
    return enableAppLogRotation;
  }

  public EventingFunctionSettings enableAppLogRotation(boolean enableAppLogRotation) {
    this.enableAppLogRotation = enableAppLogRotation;
    return this;
  }

  public String appLogDir() {
    return appLogDir;
  }

  public EventingFunctionSettings appLogDir(String appLogDir) {
    this.appLogDir = appLogDir;
    return this;
  }

  public long appLogMaxSize() {
    return appLogMaxSize;
  }

  public EventingFunctionSettings appLogMaxSize(long appLogMaxSize) {
    this.appLogMaxSize = appLogMaxSize;
    return this;
  }

  public long appLogMaxFiles() {
    return appLogMaxFiles;
  }

  public EventingFunctionSettings appLogMaxFiles(long appLogMaxFiles) {
    this.appLogMaxFiles = appLogMaxFiles;
    return this;
  }

  public Duration checkpointInterval() {
    return checkpointInterval;
  }

  public EventingFunctionSettings checkpointInterval(Duration checkpointInterval) {
    this.checkpointInterval = checkpointInterval;
    return this;
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
}
