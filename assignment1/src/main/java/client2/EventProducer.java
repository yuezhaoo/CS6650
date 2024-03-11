package client2;

import io.swagger.client.model.LiftRide;

import java.util.concurrent.BlockingQueue;


/**
 * @author yuezhao
 */
public class EventProducer implements Runnable {

  private final BlockingQueue<LiftRideEvent> queue;
  private final int numEvents;

  public EventProducer (BlockingQueue<LiftRideEvent> queue, int numEvents) {
    this.queue = queue;
    this.numEvents = numEvents;
  }

  public void run() {
    DataGenerator dataGenerator = new DataGenerator();

    for (int i = 0; i < numEvents; i ++) {
      int skierID = dataGenerator.generateSkierID();
      int resortID = dataGenerator.generateResortID();
      String seasonID = dataGenerator.generateSeasonID();
      String dayID = dataGenerator.generateDayID();
      LiftRide liftRide = dataGenerator.generateLiftRide();
      LiftRideEvent event = new LiftRideEvent(skierID, resortID, dayID, seasonID, liftRide);

      try {
        queue.put(event);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }
}