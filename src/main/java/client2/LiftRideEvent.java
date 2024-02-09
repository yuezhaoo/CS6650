package client2;

import io.swagger.client.model.LiftRide;

/**
 * @author yuezhao
 */
public class LiftRideEvent {

    public int skierID;
    public int resortID;
    public String dayID;
    public String seasonID;
    public LiftRide liftRide;

    public LiftRideEvent(int skierID, int resortID, String dayID, String seasonID, LiftRide liftRide) {
        this.skierID = skierID;
        this.resortID = resortID;
        this.dayID = dayID;
        this.seasonID = seasonID;
        this.liftRide = liftRide;
    }

}
