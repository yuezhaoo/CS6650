package client1;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yuezhao
 */
public class Application {

  public static void main(String[] args) {

    long start = System.currentTimeMillis();

    int numThreads = 100;
    int numEvents = 200000;

    LinkedBlockingQueue<LiftRideEvent> queue = new LinkedBlockingQueue<>();

    AtomicInteger successfulRequests = new AtomicInteger(0);
    AtomicInteger failedRequests = new AtomicInteger(0);
    AtomicLong totalDuration = new AtomicLong(0);
    AtomicInteger requestCount = new AtomicInteger(0);

    Thread producerThread = new Thread(new EventProducer(queue, numEvents));
    producerThread.start();

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);


    for (int i = 0; i < numThreads; i++) {
      executor.submit(new EventConsumer(queue, successfulRequests, failedRequests, totalDuration, requestCount));

    }

    executor.shutdown();

    try {
      if (!executor.awaitTermination(35, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException ie) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }

    long end = System.currentTimeMillis();
    long time = (end - start);

    int totalRequests = successfulRequests.get() + failedRequests.get();
    double throughput = (double) totalRequests / time * 1000;
    String formattedThroughput = String.format("%.2f", throughput);

    System.out.println("Total run time: " + time + " ms");
    System.out.println("Successful requests: " + successfulRequests.get());
    System.out.println("Failed requests: " + failedRequests.get());
    System.out.println("Throughput: " + formattedThroughput + " requests per second");
  }

}
