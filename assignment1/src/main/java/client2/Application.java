package client2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yuezhao
 */
public class Application {
  static ConcurrentLinkedQueue<Long> singleDurations = new ConcurrentLinkedQueue<>();

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


    long mean = requestCount.get() > 0 ? totalDuration.get() / requestCount.get() : 0;
    System.out.println("Mean response time: " + mean + " ms");


    List<Long> sortedDurations = new ArrayList<>(Application.singleDurations);
    Collections.sort(sortedDurations);

    double median;
    int size = sortedDurations.size();
    if (size % 2 == 0) {
      median = (sortedDurations.get(size / 2) + sortedDurations.get(size / 2 - 1)) / 2.0;
    } else {
      median = sortedDurations.get(size / 2);
    }
    System.out.println("Median response time: " + median + " ms");


    int index = (int) Math.ceil((99.0 / 100) * sortedDurations.size()) - 1;
    long p99Duration = sortedDurations.get(Math.max(index, 0));
    System.out.println("P99 response time: " + p99Duration + " ms");


    int totalRequests = successfulRequests.get() + failedRequests.get();
    double throughput = (double) totalRequests / time * 1000;
    String formattedThroughput = String.format("%.2f", throughput);
    System.out.println("Throughput: " + formattedThroughput + " requests per second");


    if (!Application.singleDurations.isEmpty()) {
      Long minResponseTime = Collections.min(Application.singleDurations);
      Long maxResponseTime = Collections.max(Application.singleDurations);

      System.out.println("Minimum response time: " + minResponseTime + " ms");
      System.out.println("Maximum response time: " + maxResponseTime + " ms");
    } else {
      System.out.println("No response times recorded to calculate min and max values.");
    }
  }
}
