package client2;

import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.api.SkiersApi;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yuezhao
 */
public class EventConsumer implements Runnable {

  private final BlockingQueue<LiftRideEvent> queue;
  private final AtomicInteger successfulRequests;
  private final AtomicInteger failedRequests;
  private final AtomicLong totalDuration;
  private final AtomicInteger requestCount;

  public EventConsumer(BlockingQueue<LiftRideEvent> queue,
                       AtomicInteger successfulRequests,
                       AtomicInteger failedRequests,
                       AtomicLong totalDuration,
                       AtomicInteger requestCount) {
    this.queue = queue;
    this.successfulRequests = successfulRequests;
    this.failedRequests = failedRequests;
    this.totalDuration = totalDuration;
    this.requestCount = requestCount;
  }

  public void run() {
    while (!Thread.currentThread().isInterrupted()) {
      LiftRideEvent event;

      try {
        event = queue.take();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        return;
      }

      SkiersApi apiInstance = new SkiersApi();
      ApiClient defaultClient = apiInstance.getApiClient();
      defaultClient.setBasePath("http://ec2-54-202-88-173.us-west-2.compute.amazonaws.com:8080/assignment1_war/");

      boolean success = false;
      int attempts = 0;

      while (!success && attempts < 5 && !Thread.currentThread().isInterrupted()) {
        long startTime = System.currentTimeMillis();
        try {
          apiInstance.writeNewLiftRide(event.liftRide, event.resortID, event.seasonID, event.dayID, event.skierID);
          long endTime = System.currentTimeMillis();
          long durationMillis = endTime - startTime;
          totalDuration.addAndGet(durationMillis);
          requestCount.incrementAndGet();
          Application.singleDurations.add(durationMillis);
          success = true;
          successfulRequests.incrementAndGet();
          WriteOutRecord.getInstance().writeRecord(startTime, "POST", durationMillis, 200);
        } catch (ApiException ex) {
          long endTime = System.currentTimeMillis();
          long durationMillis = endTime - startTime;
          totalDuration.addAndGet(durationMillis);
          requestCount.incrementAndGet();
          Application.singleDurations.add(durationMillis);

          if (ex.getCode() >= 400 && ex.getCode() < 600) {
            attempts++;
            WriteOutRecord.getInstance().writeRecord(startTime, "POST", durationMillis, ex.getCode());
            System.err.println("Error response code: " + ex.getCode() + ". Attempt " + (attempts));
            try {
              Thread.sleep(1000);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              System.err.println("Consumer was interrupted during backoff.");
              return;
            }
          } else {
            System.err.println("Unhandled ApiException: " + ex.getMessage());
            break;
          }
        }
      }

      if (!success) {
        failedRequests.incrementAndGet();
        System.err.println("Failed to send lift ride event after 5 attempts");
      }
    }
  }


}


