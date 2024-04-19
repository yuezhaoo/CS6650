package consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;


/**
 * @author yuezhao
 */
@WebServlet(name = "SkierServlet")
public class MessageProducer extends HttpServlet {

  private final static String QUEUE_NAME = "cs6650Queue";

  ConnectionFactory factory;
  Connection connection;
  Channel channel;
  boolean isConnectionEstablished = true;
  private DynamoDbClient dynamoDbClient;

  @Override
  public void init() throws ServletException {
    super.init();

    this.dynamoDbClient = DynamoDbClient.builder()
            .region(Region.of("us-west-2"))
            .build();

    factory = new ConnectionFactory();
    factory.setHost("ec2-54-188-93-177.us-west-2.compute.amazonaws.com");
    factory.setPort(5672);
    factory.setUsername("admin");
    factory.setPassword("canary21");
    factory.setVirtualHost("/");

    try {
      connection = factory.newConnection();
      channel = connection.createChannel();
      channel.queueDeclare(QUEUE_NAME, true, false, false, null);
    } catch (IOException | TimeoutException e) {
      isConnectionEstablished = false;
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
    res.setContentType("text/plain");
    String urlPath = req.getPathInfo();

    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      res.getWriter().write("missing parameters");
      return;
    }

    String[] urlParts = urlPath.split("/");

    if (getResortValid(urlParts)) {
      int uniqueSkiers = getSkiersID(urlParts[5]);
      res.setStatus(HttpServletResponse.SC_OK);
      res.getWriter().write(String.valueOf(uniqueSkiers));
    } else if (isUrlValid(urlParts)) {
      int dayVertical = getDayVertical(urlParts[7], urlParts[5]);
      res.setStatus(HttpServletResponse.SC_OK);
      res.getWriter().write(String.valueOf(dayVertical));
    } else if (getSkierValid(urlParts)) {
      int totalVertical = getTotalVertical(urlParts[1]);
      res.setStatus(HttpServletResponse.SC_OK);
      res.getWriter().write(String.valueOf(totalVertical));
    } else {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      res.getWriter().write("invalid parameters");
    }

  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
    if (!isConnectionEstablished) {
      res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      res.getWriter().write("Error establishing connection to RabbitMQ");
      return;
    }

    res.setContentType("application/json");
    String urlPath = req.getPathInfo();

    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      res.getWriter().write("missing parameters");
      return;
    }

    String[] urlParts = urlPath.split("/");

    if (!isUrlValid(urlParts)) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      res.getWriter().write("invalid parameters");
    }


    ObjectMapper mapper = new ObjectMapper();
    LiftRide liftRide;
    try {
      String payload = getJsonFromRequest(req);
      liftRide = mapper.readValue(payload, LiftRide.class);
    } catch (Exception e) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      res.getWriter().write("Error parsing payload: " + e.getMessage());
      return;
    }


    if (liftRide == null) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      res.getWriter().write("missing payload");
    } else if (!isPayloadValid(liftRide)) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      res.getWriter().write("invalid payload");
    }

    String message = fullMessage(urlParts, liftRide);

    try {
      channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
      res.setStatus(HttpServletResponse.SC_OK);
      res.getWriter().write("Message sent to RabbitMQ");
    } catch (Exception e) {
      res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      res.getWriter().write("Error sending message to RabbitMQ: " + e.getMessage());
    }
  }

  @Override
  public void destroy() {
    super.destroy();
    try {
      if (channel != null && channel.isOpen()) {
        channel.close();
      }
      if (connection != null && connection.isOpen()) {
        connection.close();
      }
    } catch (IOException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }


  private String getJsonFromRequest(HttpServletRequest req) throws IOException {
    String line = req.getReader().readLine();
    String bodyJson = "";

    while (line != null) {
      bodyJson += line;
      line = req.getReader().readLine();
    }

    return bodyJson;
  }

  private String fullMessage(String[] urlParts, LiftRide liftRide) {
    JsonObject jsonObject = new JsonObject();

    jsonObject.addProperty("resortID", Integer.parseInt(urlParts[1]));
    jsonObject.addProperty("seasonID", urlParts[3]);
    jsonObject.addProperty("dayID", urlParts[5]);
    jsonObject.addProperty("skierID", Integer.parseInt(urlParts[7]));
    jsonObject.addProperty("time", liftRide.getTime());
    jsonObject.addProperty("liftID", liftRide.getLiftID());

    return jsonObject.toString();
  }

  private boolean isUrlValid(String[] urlParts) {
    if (urlParts.length == 8) {
      try {
        int resortID = Integer.parseInt(urlParts[1]);
        int seasonID = Integer.parseInt(urlParts[3]);
        int dayID = Integer.parseInt(urlParts[5]);
        int skierID = Integer.parseInt(urlParts[7]);
        return urlParts[2].equals("seasons") &&
                urlParts[4].equals("days") &&
                urlParts[6].equals("skiers") &&
                resortID == 1 &&
                seasonID == 2024 &&
                dayID >= 1 && dayID <= 3 &&
                skierID >= 1 && skierID <= 100000;

      } catch (NumberFormatException e) {
        return false;
      }
    }
    return false;
  }

  private boolean getResortValid(String[] urlParts) {
    if (urlParts.length == 7) {
      try {
        int resortID = Integer.parseInt(urlParts[1]);
        int seasonID = Integer.parseInt(urlParts[3]);
        int dayID = Integer.parseInt(urlParts[5]);
        return urlParts[2].equals("seasons") &&
                urlParts[4].equals("day") &&
                urlParts[6].equals("skiers") &&
                resortID == 1 &&
                seasonID == 2024 &&
                dayID >= 1 && dayID <= 3;
      } catch (NumberFormatException e) {
        return false;
      }
    }
    return false;
  }

  private boolean getSkierValid(String[] urlParts) {
    if (urlParts.length == 3) {
      try {
        int skierID = Integer.parseInt(urlParts[1]);
        return urlParts[2].equals("vertical") &&
                skierID >= 1 && skierID <= 100000;
      } catch (NumberFormatException e) {
        return false;
      }
    }
    return false;
  }

  private boolean isPayloadValid(LiftRide liftRide) {
    if (liftRide != null) {
      try {
        int time = liftRide.getTime();
        int liftID = liftRide.getLiftID();
        return time >= 1 && time <= 360 &&
                liftID >= 1 && liftID <= 40;
      } catch (NumberFormatException e) {
        return false;
      }
    }
    return false;
  }

  private int getSkiersID(String dayID) {
    int uniqueSkierCount = 0;

    try {
      QueryRequest queryRequest = QueryRequest.builder()
              .tableName("UniqueSkiers")
              .keyConditionExpression("dayID = :dayID")
              .expressionAttributeValues(Map.of(":dayID", AttributeValue.builder().s(dayID).build()))
              .projectionExpression("uniqueSkierCount")
              .build();

      QueryResponse queryResponse = dynamoDbClient.query(queryRequest);

      if (!queryResponse.items().isEmpty()) {
        Map<String, AttributeValue> item = queryResponse.items().get(0);
        uniqueSkierCount = Integer.parseInt(item.get("uniqueSkierCount").s());
      }
    } catch (DynamoDbException e) {
      e.printStackTrace();
    }

    return uniqueSkierCount;
  }

  private int getDayVertical(String skierID, String dayID) {
    int dayVertical = 0;

    try {
      QueryRequest queryRequest = QueryRequest.builder()
              .tableName("cs6650Table")
              .indexName("skierID-dayID-index")
              .keyConditionExpression("skierID = :skierIdVal and dayID = :dayIdVal")
              .expressionAttributeValues(Map.of(
                      ":skierIdVal", AttributeValue.builder().n(skierID).build(),
                      ":dayIdVal", AttributeValue.builder().s(dayID).build()
              ))
              .projectionExpression("liftID")
              .build();

      QueryResponse queryResponse = dynamoDbClient.query(queryRequest);

      for (Map<String, AttributeValue> item : queryResponse.items()) {
        int liftId = Integer.parseInt(item.get("liftID").n());
        dayVertical += liftId * 10;
      }

    } catch (DynamoDbException e) {
      e.printStackTrace();
    }

    return dayVertical;
  }

  private int getTotalVertical(String skierID) {
    int totalVertical = 0;

    try {
      QueryRequest queryRequest = QueryRequest.builder()
              .tableName("cs6650Table")
              .indexName("skierID-index")
              .keyConditionExpression("skierID = :skierIdVal")
              .expressionAttributeValues(Map.of(":skierIdVal", AttributeValue.builder().n(skierID).build()))
              .projectionExpression("liftID")
              .build();

      QueryResponse queryResponse = dynamoDbClient.query(queryRequest);

      for (Map<String, AttributeValue> item : queryResponse.items()) {
        int liftId = Integer.parseInt(item.get("liftID").n());
        totalVertical += liftId * 10;
      }

    } catch (DynamoDbException e) {
      e.printStackTrace();
    }

    return totalVertical;
  }


}


