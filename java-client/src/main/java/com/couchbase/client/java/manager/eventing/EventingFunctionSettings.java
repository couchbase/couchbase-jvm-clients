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

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

public class EventingFunctionSettings {

  private final EventingFunctionProcessingStatus processingStatus;
  private final EventingFunctionDeploymentStatus deploymentStatus;
  private final long cppWorkerThreadCount;
  private final EventingFunctionDcpBoundary dcpStreamBoundary;
  private final String description;
  private final EventingFunctionLogLevel logLevel;
  private final EventingFunctionLanguageCompatibility languageCompatibility;
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

  /**
   * Creates the {@link EventingFunctionSettings} with default properties.
   *
   * @return the build settings.
   */
  public static EventingFunctionSettings create() {
    return builder().build();
  }

  /**
   * Creates the {@link Builder} which allows to customize the settings.
   *
   * @return the {@link Builder}.
   */
  public static Builder builder() {
    return new Builder();
  }

  private EventingFunctionSettings(final Builder builder) {
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

  /**
   * Indicates if the function is running (i.e., not paused).
   */
  public EventingFunctionProcessingStatus processingStatus() {
    return processingStatus;
  }

  /**
   * Indicates if the function is deployed.
   */
  public EventingFunctionDeploymentStatus deploymentStatus() {
    return deploymentStatus;
  }

  /**
   * Number of threads each worker utilizes.
   */
  public long cppWorkerThreadCount() {
    return cppWorkerThreadCount;
  }

  /**
   * Indicates where to start dcp stream from.
   */
  public EventingFunctionDcpBoundary dcpStreamBoundary() {
    return dcpStreamBoundary;
  }

  /**
   * Free form text for user to describe the handler. no functional role.
   */
  public String description() {
    return description;
  }

  /**
   * Level of detail in system logging.
   */
  public EventingFunctionLogLevel logLevel() {
    return logLevel;
  }

  /**
   * Eventing language version this handler assumes in terms of syntax and behavior.
   */
  public EventingFunctionLanguageCompatibility languageCompatibility() {
    return languageCompatibility;
  }

  /**
   * Maximum time the handler can run before it is forcefully terminated.
   */
  public Duration executionTimeout() {
    return executionTimeout;
  }

  /**
   * Maximum number of libcouchbase connections that may be opened and pooled.
   */
  public long lcbInstCapacity() {
    return lcbInstCapacity;
  }

  /**
   * Number of retries of retryable libcouchbase failures.
   */
  public long lcbRetryCount() {
    return lcbRetryCount;
  }

  /**
   * Maximum time the lcb command is waited until completion before we terminate the request.
   */
  public Duration lcbTimeout() {
    return lcbTimeout;
  }

  /**
   * Consistency level used by n1ql statements in the handler.
   */
  public QueryScanConsistency queryConsistency() {
    return queryConsistency;
  }

  /**
   * Number of timer shards. defaults to number of vbuckets.
   */
  public long numTimerPartitions() {
    return numTimerPartitions;
  }

  /**
   * Batch size for messages from producer to consumer.
   */
  public long sockBatchSize() {
    return sockBatchSize;
  }

  /**
   * Duration to log stats from this handler.
   */
  public Duration tickDuration() {
    return tickDuration;
  }

  /**
   * Size limit of timer context object.
   */
  public long timerContextSize() {
    return timerContextSize;
  }

  /**
   * Key prefix for all data stored in metadata by this handler.
   */
  public String userPrefix() {
    return userPrefix;
  }

  /**
   * Maximum size in bytes the bucket cache can grow to.
   */
  public long bucketCacheSize() {
    return bucketCacheSize;
  }

  /**
   * Time in milliseconds after which a cached bucket object is considered stale.
   */
  public long bucketCacheAge() {
    return bucketCacheAge;
  }

  /**
   * Maximum allowable curl call response in 'MegaBytes'.
   */
  public long curlMaxAllowedRespSize() {
    return curlMaxAllowedRespSize;
  }

  /**
   * Number of worker processes handler utilizes on each eventing node.
   */
  public long workerCount() {
    return workerCount;
  }

  /**
   * Automatically prepare all n1ql statements in the handler.
   */
  public boolean queryPrepareAll() {
    return queryPrepareAll;
  }

  /**
   * Code to automatically prepend to top of handler code.
   */
  public List<String> handlerHeaders() {
    return handlerHeaders;
  }

  /**
   * Code to automatically append to bottom of handler code.
   */
  public List<String> handlerFooters() {
    return handlerFooters;
  }

  /**
   * Enable rotating this handlers log() message files.
   */
  public boolean enableAppLogRotation() {
    return enableAppLogRotation;
  }

  /**
   * Directory to write content of log() message files.
   */
  public String appLogDir() {
    return appLogDir;
  }

  /**
   * Rotate logs when file grows to this size in bytes approximately.
   */
  public long appLogMaxSize() {
    return appLogMaxSize;
  }

  /**
   * Number of log() message files to retain when rotating.
   */
  public long appLogMaxFiles() {
    return appLogMaxFiles;
  }

  /**
   * Number of seconds before writing a progress checkpoint.
   */
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
    private EventingFunctionDcpBoundary dcpStreamBoundary;
    private String description;
    private EventingFunctionLogLevel logLevel;
    private EventingFunctionLanguageCompatibility languageCompatibility;
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

    /**
     * (internal) sets the processing status received from the server.
     *
     * @param processingStatus the status received from the server.
     * @return this {@link Builder} for chaining purposes.
     */
    Builder processingStatus(EventingFunctionProcessingStatus processingStatus) {
      this.processingStatus = notNull(processingStatus, "ProcessingStatus");
      return this;
    }

    /**
     * (internal) sets the deployment status received from the server.
     *
     * @param deploymentStatus the status received from the server.
     * @return this {@link Builder} for chaining purposes.
     */
    Builder deploymentStatus(EventingFunctionDeploymentStatus deploymentStatus) {
      this.deploymentStatus = notNull(deploymentStatus, "DeploymentStatus");
      return this;
    }

    /**
     * Number of seconds before writing a progress checkpoint.
     *
     * @param checkpointInterval number of seconds before writing a progress checkpoint.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder checkpointInterval(Duration checkpointInterval) {
      this.checkpointInterval = notNull(checkpointInterval, "CheckpointInterval");
      return this;
    }

    /**
     * Number of log() message files to retain when rotating.
     *
     * @param appLogMaxFiles number of log() message files to retain when rotating.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder appLogMaxFiles(long appLogMaxFiles) {
      this.appLogMaxFiles = appLogMaxFiles;
      return this;
    }

    /**
     * Rotate logs when file grows to this size in bytes approximately.
     *
     * @param appLogMaxSize rotate logs when file grows to this size in bytes approximately.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder appLogMaxSize(long appLogMaxSize) {
      this.appLogMaxSize = appLogMaxSize;
      return this;
    }

    /**
     * Directory to write content of log() message files.
     *
     * @param appLogDir directory to write content of log() message files.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder appLogDir(String appLogDir) {
      this.appLogDir = notNullOrEmpty(appLogDir, "AppLogDir");
      return this;
    }

    /**
     * Enable rotating this handlers log() message files.
     *
     * @param enableAppLogRotation enable rotating this handlers log() message files.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder enableAppLogRotation(boolean enableAppLogRotation) {
      this.enableAppLogRotation = enableAppLogRotation;
      return this;
    }

    /**
     * Code to automatically append to bottom of handler code.
     *
     * @param handlerFooters code to automatically append to bottom of handler code.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder handlerFooters(List<String> handlerFooters) {
      this.handlerFooters = notNull(handlerFooters, "HandlerFooters");
      return this;
    }

    /**
     * Code to automatically prepend to top of handler code.
     *
     * @param handlerHeaders code to automatically prepend to top of handler code.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder handlerHeaders(List<String> handlerHeaders) {
      this.handlerHeaders = notNull(handlerHeaders, "HandlerHeaders");
      return this;
    }

    /**
     * Automatically prepare all n1ql statements in the handler.
     *
     * @param queryPrepareAll automatically prepare all n1ql statements in the handler.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder queryPrepareAll(boolean queryPrepareAll) {
      this.queryPrepareAll = queryPrepareAll;
      return this;
    }

    /**
     * Number of worker processes handler utilizes on each eventing node.
     *
     * @param workerCount number of worker processes handler utilizes on each eventing node.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder workerCount(long workerCount) {
      this.workerCount = workerCount;
      return this;
    }

    /**
     * Number of threads each worker utilizes.
     *
     * @param cppWorkerThreadCount number of threads each worker utilizes.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder cppWorkerThreadCount(long cppWorkerThreadCount) {
      this.cppWorkerThreadCount = cppWorkerThreadCount;
      return this;
    }

    /**
     * Indicates where to start dcp stream from (beginning of time, present point).
     *
     * @param dcpStreamBoundary indicates where to start dcp stream from (beginning of time, present point).
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder dcpStreamBoundary(EventingFunctionDcpBoundary dcpStreamBoundary) {
      this.dcpStreamBoundary = notNull(dcpStreamBoundary, "DcpStreamBoundary");
      return this;
    }

    /**
     * Free form text for user to describe the handler. no functional role.
     *
     * @param description free form text for user to describe the handler. no functional role.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder description(String description) {
      this.description = notNullOrEmpty(description, "Description");
      return this;
    }

    /**
     * Level of detail in system logging.
     *
     * @param logLevel level of detail in system logging.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder logLevel(EventingFunctionLogLevel logLevel) {
      this.logLevel = notNull(logLevel, "LogLevel");
      return this;
    }

    /**
     * Eventing language version this handler assumes in terms of syntax and behavior.
     *
     * @param languageCompatibility eventing language version this handler assumes in terms of syntax and behavior.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder languageCompatibility(EventingFunctionLanguageCompatibility languageCompatibility) {
      this.languageCompatibility = notNull(languageCompatibility, "EventingFunctionLanguageCompatibility");
      return this;
    }

    /**
     * Maximum time the handler can run before it is forcefully terminated.
     *
     * @param executionTimeout maximum time the handler can run before it is forcefully terminated.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder executionTimeout(Duration executionTimeout) {
      this.executionTimeout = notNull(executionTimeout, "ExecutionTimeout");
      return this;
    }

    /**
     * Maximum number of libcouchbase connections that may be opened and pooled.
     *
     * @param lcbInstCapacity maximum number of libcouchbase connections that may be opened and pooled.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder lcbInstCapacity(long lcbInstCapacity) {
      this.lcbInstCapacity = lcbInstCapacity;
      return this;
    }

    /**
     * Number of retries of retriable libcouchbase failures. 0 keeps trying till execution_timeout.
     *
     * @param lcbRetryCount number of retries of retriable libcouchbase failures. 0 keeps trying till execution_timeout.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder lcbRetryCount(long lcbRetryCount) {
      this.lcbRetryCount = lcbRetryCount;
      return this;
    }

    /**
     * Maximum time the lcb command is waited until completion before we terminate the request.
     *
     * @param lcbTimeout maximum time the lcb command is waited until completion before we terminate the request.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder lcbTimeout(Duration lcbTimeout) {
      this.lcbTimeout = notNull(lcbTimeout, "LcbTimeout");
      return this;
    }

    /**
     * Consistency level used by n1ql statements in the handler.
     *
     * @param queryConsistency consistency level used by n1ql statements in the handler.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder queryConsistency(QueryScanConsistency queryConsistency) {
      this.queryConsistency = notNull(queryConsistency, "QueryConsistency");
      return this;
    }

    /**
     * Number of timer shards. defaults to number of vbuckets.
     *
     * @param numTimerPartitions number of timer shards. defaults to number of vbuckets.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder numTimerPartitions(long numTimerPartitions) {
      this.numTimerPartitions = numTimerPartitions;
      return this;
    }

    /**
     * Batch size for messages from producer to consumer. normally, this must not be specified.
     *
     * @param sockBatchSize batch size for messages from producer to consumer. normally, this must not be specified.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder sockBatchSize(long sockBatchSize) {
      this.sockBatchSize = sockBatchSize;
      return this;
    }

    /**
     * Duration to log stats from this handler.
     *
     * @param tickDuration duration to log stats from this handler.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder tickDuration(Duration tickDuration) {
      this.tickDuration = notNull(tickDuration, "Tick Duration");
      return this;
    }

    /**
     * Size limit of timer context object.
     *
     * @param timerContextSize size limit of timer context object.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder timerContextSize(long timerContextSize) {
      this.timerContextSize = timerContextSize;
      return this;
    }

    /**
     * Key prefix for all data stored in metadata by this handler.
     *
     * @param userPrefix key prefix for all data stored in metadata by this handler.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder userPrefix(String userPrefix) {
      this.userPrefix = notNullOrEmpty(userPrefix, "User Prefix");
      return this;
    }

    /**
     * Maximum size in bytes the bucket cache can grow to.
     *
     * @param bucketCacheSize maximum size in bytes the bucket cache can grow to.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder bucketCacheSize(long bucketCacheSize) {
      this.bucketCacheSize = bucketCacheSize;
      return this;
    }

    /**
     * Time in milliseconds after which a cached bucket object is considered stale.
     *
     * @param bucketCacheAge time in milliseconds after which a cached bucket object is considered stale.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder bucketCacheAge(long bucketCacheAge) {
      this.bucketCacheAge = bucketCacheAge;
      return this;
    }

    /**
     * Maximum allowable curl call response in 'MegaBytes'.
     * <p>
     * Setting the value to 0 lifts the upper limit off. This parameter affects v8 engine stability since it
     * defines the maximum amount of heap space acquired by a curl call.
     *
     * @param curlMaxAllowedRespSize maximum allowable curl call response in 'MegaBytes'.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder curlMaxAllowedRespSize(long curlMaxAllowedRespSize) {
      this.curlMaxAllowedRespSize = curlMaxAllowedRespSize;
      return this;
    }

    /**
     * Builds the {@link EventingFunctionSettings}.
     */
    public EventingFunctionSettings build() {
      return new EventingFunctionSettings(this);
    }

  }
}
