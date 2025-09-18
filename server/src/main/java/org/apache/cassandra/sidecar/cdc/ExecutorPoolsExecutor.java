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
