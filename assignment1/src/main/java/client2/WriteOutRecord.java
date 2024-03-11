package client2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author yuezhao
 */
public class WriteOutRecord {

  private static WriteOutRecord instance;
  private BufferedWriter bufferedWriter;
  private FileWriter fileWriter;

  private WriteOutRecord() {
    try {
      fileWriter = new FileWriter("record.csv");
      bufferedWriter = new BufferedWriter(fileWriter);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static synchronized WriteOutRecord getInstance() {
    if (instance == null) {
      instance = new WriteOutRecord();
    }
    return instance;
  }

  public synchronized void writeRecord(long startTime, String requestType, long latency, int responseCode) {
    try {
      String record = String.format("%d,%s,%d,%d\n", startTime, requestType, latency, responseCode);
      bufferedWriter.write(record);
      bufferedWriter.flush();
    } catch (IOException e) {
      System.err.println("Error writing to CSV file: " + e.getMessage());
    }
  }

  public void close() {
    try {
      bufferedWriter.close();
      fileWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
