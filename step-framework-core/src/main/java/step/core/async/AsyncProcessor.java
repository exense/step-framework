package step.core.async;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public class AsyncProcessor<T> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(AsyncProcessor.class);
    // Default maximum queue size
    private static final int MAX_QUEUE_SIZE = 5000;
    // FIFO queue to store object with a configurable capacity
    private final BlockingQueue<T> queue;
    // Consumer of the queue objects
    private final Consumer<T> consumer;
    private final Thread workerThread;

    // Flag to control worker thread
    private volatile boolean running = true;
    private volatile boolean processing = false;

    public AsyncProcessor(int maxSize, Consumer<T> consumer) {
        // Use default when maxSize is not set (only the case for JUnit tests)
        queue = new LinkedBlockingQueue<>((maxSize > 0) ? maxSize : MAX_QUEUE_SIZE);
        this.consumer = consumer;
        // Start a background worker thread
        workerThread = new Thread(this::processQueue);
        workerThread.setDaemon(true); // Ensures the thread stops when the program exits
        workerThread.start();
    }

    // Method to enqueue an object
    public void enqueue(T object) {
        if (running) {
            try {
                queue.put(object); // Adds object to the queue (FIFO)
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore interrupted status
            }
        } else {
            throw new UnsupportedOperationException("The async processor is stopped");
        }
    }

    // Method to process the objects in the queue asynchronously
    private void processQueue() {
        while (running || !queue.isEmpty()) {
            try {
                processing = false;
                T object = queue.take(); // Retrieves and removes the head of the queue (blocking)
                processing = true;
                consumer.accept(object);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore interrupted status
                break;
            } catch (Throwable e){
                logger.error("An exception occurred while asynchronously processing an element of the queue.", e);
            }
        }
    }

    // Method to stop the processor gracefully
    // could implement serialization here and reload from file at startup, currently still handling it gracefully by draining the queue
    private void stop() {
        running = false;
        logger.debug("Stopping async processor thread, current queue size {}", queue.size());
        //enqueue is now blocked, dequeue might still have elements to be processed.
        //if queue is not empty, thread is not blocked in take, we can safely wait for the thread to terminate
        //if queue is empty but last element is being process, we also wait for the thread to terminate
        if (!queue.isEmpty() || processing) {
            try {
                long start = System.currentTimeMillis();
                workerThread.join(30000L);
                long joinCompleted = System.currentTimeMillis();
                logger.debug("Waited on async thread to stop for {}ms, queue size is {}", joinCompleted - start, queue.size());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore interrupted status
            }
        } else {
            //queue is empty and not processing is ongoing, just interrupt the thread in case it's waiting in take
            workerThread.interrupt();
        }
    }

    @Override
    public void close() throws Exception {
        stop();
    }
}
