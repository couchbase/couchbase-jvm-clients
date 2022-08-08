/*
 * Copyright (c) 2022 Couchbase, Inc.
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
package com.couchbase.client.performer.core.commands;

import com.couchbase.client.performer.core.perf.Counters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class TransactionCommandExecutor extends Executor {
    protected final Logger logger = LoggerFactory.getLogger(TransactionCommandExecutor.class);

    public TransactionCommandExecutor(Counters counters) {
        super(counters);
    }

    abstract protected com.couchbase.client.protocol.run.Result performOperation(com.couchbase.client.protocol.transactions.TransactionCreateRequest op, boolean performanceMode);

    public com.couchbase.client.protocol.run.Result run(com.couchbase.client.protocol.transactions.TransactionCreateRequest command, boolean performanceMode) {
        return performOperation(command, performanceMode);
    }
}
