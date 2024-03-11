package consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.json.JSONObject;


public class MessageConsumer implements Runnable {

  private final String queueName;
  private final ConcurrentHashMap<Integer, List<JSONObject>> eventMap;
  Channel channel;

  public MessageConsumer(String queueName,
                         ConcurrentHashMap<Integer, List<JSONObject>> eventMap,
                         Channel channel) {
    this.queueName = queueName;
    this.eventMap = eventMap;
    this.channel = channel;
  }

  @Override
  public void run() {

    try {
      DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), "UTF-8");
        processMessage(message);
      };

      channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
      });
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void processMessage(String message) {
    JSONObject event = new JSONObject(message);
    int skierId = event.getInt("skierID");

    eventMap.compute(skierId, (key, list) -> {
      if (list == null) {
        list = new ArrayList<>();
      }
      list.add(event);
      return list;
    });
  }

  public static void main(String[] args) throws IOException, TimeoutException {
    int numThreads = 100;
    String QUEUE_NAME = "cs6650Queue";
    ConcurrentHashMap<Integer, List<JSONObject>> eventMap = new ConcurrentHashMap<>();

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("ec2-34-212-29-75.us-west-2.compute.amazonaws.com");
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

    for (int i = 0; i < numThreads; i++) {
      executorService.submit(new MessageConsumer(QUEUE_NAME, eventMap, channel));
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
  }
}
