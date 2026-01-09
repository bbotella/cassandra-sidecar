/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.cdc;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.spark.utils.AsyncExecutor;

/**
 * Wrapper to pass an executor pool to cdc classes.
 */
public class ExecutorPoolsExecutor implements AsyncExecutor
{
    public ExecutorPoolsExecutor(TaskExecutorPool executorPool)
    {
        this.executorPool = executorPool;
    }

    private final TaskExecutorPool executorPool;

    @Override
    public <T> CompletableFuture<T> submit(Supplier<T> blockingAction)
    {
        try
        {
            return executorPool.<T>executeBlocking(promise -> promise.complete(blockingAction.get()), false)
                               .toCompletionStage()
                               .toCompletableFuture();
        }
        catch (Exception e)
        {
            return CompletableFuture.failedFuture(e);
        }
    }

    public <T> CompletableFuture<Void> submit(Runnable blockingAction)
    {
        return executorPool.executeBlocking((promise) -> {
                               blockingAction.run();
                               promise.complete();
                           }, false).toCompletionStage().toCompletableFuture()
                           .thenApply(a -> null);
    }

    public <T> CompletableFuture<Void> schedule(Runnable task, long delayMillis)
    {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        executorPool.setTimer(delayMillis, (timerId) -> {
            try
            {
                task.run();
                future.complete(null);
            }
            catch (Throwable t)
            {
                future.completeExceptionally(t);
            }
        });
        return future;
    }

    public long periodicTimer(Runnable task, long delayMillis)
    {
        return executorPool.setPeriodic(delayMillis, (promise) -> task.run());
    }

    public boolean cancelTimer(long timerId)
    {
        return executorPool.cancelTimer(timerId);
    }

    static ExecutorPoolsExecutor wrap(TaskExecutorPool executorPool)
    {
        return new ExecutorPoolsExecutor(executorPool);
    }
}
