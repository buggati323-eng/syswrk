package uk.ac.ntu.cloudfs.lb.core;

import java.util.concurrent.*;

public final class JobQueue implements AutoCloseable {

    private final BlockingQueue<Runnable> queue;
    private final ThreadPoolExecutor exec;

    public JobQueue(int workers, int queueCapacity) {
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.exec = new ThreadPoolExecutor(
                workers, workers,
                0L, TimeUnit.MILLISECONDS,
                this.queue,
                new ThreadPoolExecutor.AbortPolicy() // reject when full
        );
    }

    public int queued() {
        return queue.size();
    }

    public int capacity() {
        return queue.remainingCapacity() + queue.size();
    }

    public <T> Future<T> submit(Callable<T> task) throws RejectedExecutionException {
        return exec.submit(task);
    }

    @Override
    public void close() {
        exec.shutdownNow();
    }
}
