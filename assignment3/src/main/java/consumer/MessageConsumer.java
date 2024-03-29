package consumer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.json.JSONObject;



public class MessageConsumer implements Runnable {
  private final String queueName;
  private final DynamoDbClient dynamoDbClient;
  private static final String tableName = "cs6650Table";
  private final Channel channel;
  private final ExecutorService batchWriteExecutor;
  private List<WriteRequest> writeRequests = new ArrayList<>();
  private final int BATCH_SIZE = 25;
  private final int MAX_ACCUMULATED_SIZE = 500;

  public MessageConsumer(String queueName, DynamoDbClient dynamoDbClient, Channel channel, ExecutorService batchWriteExecutor) {
    this.queueName = queueName;
    this.dynamoDbClient = dynamoDbClient;
    this.channel = channel;
    this.batchWriteExecutor = batchWriteExecutor;
  }

  @Override
  public void run() {
    try {
      DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), "UTF-8");
        processMessage(message);
      };
      channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  private void processMessage(String message) {
    JSONObject event = new JSONObject(message);
    String skierId = String.valueOf(event.getInt("skierID"));
    String resortId = String.valueOf(event.getInt("resortID"));
    String seasonId = event.getString("seasonID");
    String dayId = event.getString("dayID");
    int time = event.getInt("time");
    int liftId = event.getInt("liftID");

    Map<String, AttributeValue> itemAttributes = new HashMap<>();
    itemAttributes.put("skierID", AttributeValue.builder().s(skierId).build());
    itemAttributes.put("resortID", AttributeValue.builder().s(resortId).build());
    itemAttributes.put("seasonID", AttributeValue.builder().s(seasonId).build());
    itemAttributes.put("dayID", AttributeValue.builder().s(dayId).build());
    itemAttributes.put("time", AttributeValue.builder().n(String.valueOf(time)).build());
    itemAttributes.put("liftID", AttributeValue.builder().n(String.valueOf(liftId)).build());

    synchronized (writeRequests) {
      writeRequests.add(WriteRequest.builder().putRequest(PutRequest.builder().item(itemAttributes).build()).build());
      if (writeRequests.size() >= MAX_ACCUMULATED_SIZE) {
        List<WriteRequest> batchToWrite = new ArrayList<>(writeRequests);
        writeRequests.clear();
        batchWriteExecutor.submit(() -> batchWrite(batchToWrite));
      }
    }
  }


  private void batchWrite(List<WriteRequest> batchToWrite) {
    try {
      for (int i = 0; i < batchToWrite.size(); i += BATCH_SIZE) {
        int end = Math.min(i + BATCH_SIZE, batchToWrite.size());
        List<WriteRequest> batch = batchToWrite.subList(i, end);
        BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                .requestItems(Map.of(tableName, batch))
                .build();
        dynamoDbClient.batchWriteItem(batchWriteItemRequest);
      }
    } catch (DynamoDbException e) {
      System.err.println(e.getMessage());
    }
  }


  public void flushRemainingWrites() {
    synchronized (this) {
      if (!writeRequests.isEmpty()) {
        batchWriteExecutor.submit(() -> batchWrite(new ArrayList<>(writeRequests)));
        writeRequests.clear();
      }
    }
  }


  public static void main(String[] args) throws IOException, TimeoutException {
    String accessKey = "ASIAQZQ3MPDKAHTDTLUB";
    String secretKey = "htf5dg8wuYVKr8DOLnLyAmWqpEJQt2FEbCzGeyfs";
    String sessionToken = "FwoGZXIvYXdzEK3//////////wEaDA8jliRYbLRk3Nan0CLLAcn3pPbc+tE5/5yPzlqxAnAy3wJs8txehJXEkwg7owUwgHcYcei6jVLe/mNH1Zz7FEKhU0iY4xYM/aLIbTh9l1yE23I+jIMjeMxqdqzLZxz7mV/quJarSbdRdSao5Vuq7DkPt+YQ68wNhapKWbN3nG+HTv0hUazxMYXK7LebDnPGnt3OQ7rU6jjth+pfAFQ2TXux2zzgjHuG/yIrlM+Xhascx13Hbf0yvEp9glzX5iKvRrQ8ypm82QP9TmQJpHXuZjoKVW4fwmuw2LkTKL6qjrAGMi1X9YKXopQqayTUt060Rx2F5AYqzgoxHwR2tq5thlDP3nltX4/aRAiewFvmAm4=";
    AwsCredentials awsCreds = AwsSessionCredentials.create(accessKey, secretKey, sessionToken);

    int numThreads = 100;
    String QUEUE_NAME = "cs6650Queue";

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("ec2-18-236-87-144.us-west-2.compute.amazonaws.com");
    factory.setPort(5672);
    factory.setUsername("admin");
    factory.setPassword("canary21");
    factory.setVirtualHost("/");
    Connection connection;
    Channel channel;

    try {
      connection = factory.newConnection();
      channel = connection.createChannel();
      channel.queueDeclare(QUEUE_NAME, true, false, false, null);
    } catch (IOException | TimeoutException e) {
      throw new RuntimeException(e);
    }

    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    ExecutorService writeExecutor = Executors.newFixedThreadPool(numThreads);

    DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
            .region(Region.US_WEST_2)
            .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
            .build();


    List<MessageConsumer> consumers = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      MessageConsumer consumer = new MessageConsumer(QUEUE_NAME, dynamoDbClient, channel, writeExecutor);
      consumers.add(consumer);
      executorService.submit(consumer);
    }

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      for (MessageConsumer consumer : consumers) {
        consumer.flushRemainingWrites();
      }
      writeExecutor.shutdown();
      try {
        if (!writeExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
          writeExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        writeExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }));



    executorService.shutdown();

    try {
      if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
