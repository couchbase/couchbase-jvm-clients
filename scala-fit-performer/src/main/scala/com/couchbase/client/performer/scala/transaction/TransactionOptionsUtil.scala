/*
 * Copyright 2025 Couchbase, Inc.
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
// [if:1.9.0]
package com.couchbase.client.performer.scala.transaction

import com.couchbase.client.performer.scala.ScalaSdkCommandExecutor
import com.couchbase.client.protocol.transactions._
import com.couchbase.client.scala.transactions.config.{TransactionGetOptions, TransactionGetReplicaFromPreferredServerGroupOptions, TransactionInsertOptions, TransactionReplaceOptions}

object TransactionOptionsUtil {

  def transactionReplaceOptions(request: CommandReplace): Option[TransactionReplaceOptions] = {
    if (request.hasOptions) {
      val opts = request.getOptions
      if (opts.hasTranscoder) {
        var options = TransactionReplaceOptions.Default
        options = options.transcoder(ScalaSdkCommandExecutor.convertTranscoder(opts.getTranscoder))
        Some(options)
      } else {
        None
      }
    } else {
      None
    }
  }

  def transactionReplaceOptions(request: Replace): Option[TransactionReplaceOptions] = {
    if (request.hasOptions) {
      val opts = request.getOptions
      if (opts.hasTranscoder) {
        var options = TransactionReplaceOptions.Default
        options = options.transcoder(ScalaSdkCommandExecutor.convertTranscoder(opts.getTranscoder))
        Some(options)
      } else {
        None
      }
    } else {
      None
    }
  }

  def transactionInsertOptions(request: CommandInsert): Option[TransactionInsertOptions] = {
    if (request.hasOptions) {
      val opts = request.getOptions
      if (opts.hasTranscoder) {
        var options = TransactionInsertOptions.Default
        options = options.transcoder(ScalaSdkCommandExecutor.convertTranscoder(opts.getTranscoder))
        Some(options)
      } else {
        None
      }
    } else {
      None
    }
  }

  def transactionInsertOptions(request: Insert): Option[TransactionInsertOptions] = {
    if (request.hasOptions) {
      val opts = request.getOptions
      if (opts.hasTranscoder) {
        var options = TransactionInsertOptions.Default
        options = options.transcoder(ScalaSdkCommandExecutor.convertTranscoder(opts.getTranscoder))
        Some(options)
      } else {
        None
      }
    } else {
      None
    }
  }

  def transactionGetOptions(request: CommandGet): Option[TransactionGetOptions] = {
    if (request.hasOptions) {
      val opts = request.getOptions
      if (opts.hasTranscoder) {
        var options = TransactionGetOptions.Default
        options = options.transcoder(ScalaSdkCommandExecutor.convertTranscoder(opts.getTranscoder))
        Some(options)
      } else {
        None
      }
    } else {
      None
    }
  }

  def transactionGetOptions(request: Get): Option[TransactionGetOptions] = {
    if (request.hasOptions) {
      val opts = request.getOptions
      if (opts.hasTranscoder) {
        var options = TransactionGetOptions.Default
        options = options.transcoder(ScalaSdkCommandExecutor.convertTranscoder(opts.getTranscoder))
        Some(options)
      } else {
        None
      }
    } else {
      None
    }
  }

  def transactionGetReplicaFromPreferredServerGroupOptions(request: CommandGetReplicaFromPreferredServerGroup): Option[TransactionGetReplicaFromPreferredServerGroupOptions] = {
    if (request.hasOptions) {
      val opts = request.getOptions
      if (opts.hasTranscoder) {
        var options = TransactionGetReplicaFromPreferredServerGroupOptions.Default
        options = options.transcoder(ScalaSdkCommandExecutor.convertTranscoder(opts.getTranscoder))
        Some(options)
      } else {
        None
      }
    } else {
      None
    }
  }
}
// [end]