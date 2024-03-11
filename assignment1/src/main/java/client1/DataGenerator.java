package client1;

import io.swagger.client.model.LiftRide;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author yuezhao
 */
public class DataGenerator {

  public int generateSkierID() {
    return ThreadLocalRandom.current().nextInt(1, 100001);
  }

  public int generateResortID() {
    return ThreadLocalRandom.current().nextInt(1, 11);
  }

  public int generateLiftID() {
    return ThreadLocalRandom.current().nextInt(1, 41);
  }

  public String generateSeasonID() {
    return Integer.toString(2024) ;
  }

  public String generateDayID() {
    return Integer.toString(1) ;
  }

  public int generateTime() {
    return ThreadLocalRandom.current().nextInt(1, 361);
  }

  public LiftRide generateLiftRide() {
    LiftRide liftRide = new LiftRide();
    liftRide.setLiftID(generateLiftID());
    liftRide.setTime(generateTime());

    return liftRide;
  }

}
