import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

  private static final int PORT = 6379;

  public static void main(String[] args){
    ServerSocket serverSocket;
    try {
      serverSocket = new ServerSocket(PORT);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);
      // Wait for connection from client and handle
      listenAndHandleConnections(serverSocket);
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }

  private static void listenAndHandleConnections(ServerSocket serverSocket) {
    try (ExecutorService executor = Executors.newFixedThreadPool(10)) {
      while (true) {
        executor.submit(new ConnectionHandler(serverSocket.accept()));
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }

  private static void handleConnection(Socket clientSocket) throws IOException {
    try (clientSocket) {
      BufferedReader reader = new BufferedReader(
          new InputStreamReader(clientSocket.getInputStream()));
      BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(clientSocket.getOutputStream()));
      // Main listening loop
      while (true) {
        List<String> commandArray = RespUtil.readArray(reader);
        System.out.println("Received command: " + String.join(",", commandArray));
        String reply = processSimpleCommand(commandArray);
        System.out.println("Sending reply: " + reply);
        writer.write(reply);
        writer.flush();
      }
    } catch (IOException | RespException e) {
      System.out.println("Exception: " + e.getMessage());
      e.printStackTrace();
    }
  }

  private record RedisValue(String value, Long expiryTime) {}
  private static final Map<String, RedisValue> keyValueStore = new HashMap<>();

  private static String processSimpleCommand(List<String> commandArray) {
    String command = commandArray.getFirst().toUpperCase();
    switch (command) {
      case "PING":
        return RespUtil.serializeBulkString("PONG");
      case "ECHO":
        String payload = commandArray.get(1);
        return RespUtil.serializeBulkString(payload);
      case "SET":
        String keyToSet = commandArray.get(1);
        String valueToSet = commandArray.get(2);
        Long expiryTime = null;
        if (commandArray.size() > 3 && commandArray.get(3).equalsIgnoreCase("PX")) {
          // TODO handle error if expiry time is not a number
          long expiryInMillis = Long.parseLong(commandArray.get(4));
          expiryTime = System.currentTimeMillis() + expiryInMillis;
        }
        keyValueStore.put(keyToSet, new RedisValue(valueToSet, expiryTime));
        return RespUtil.serializeBulkString("OK");
      case "GET":
        String keyToGet = commandArray.get(1);
        RedisValue valueToGet = keyValueStore.get(keyToGet);
        if (valueToGet.expiryTime != null && valueToGet.expiryTime < System.currentTimeMillis()) {
          keyValueStore.remove(keyToGet);
          return RespUtil.serializeBulkString(null);
        }
        return RespUtil.serializeBulkString(valueToGet.value);
      default:
        return "";
    }
  }

  private record ConnectionHandler(Socket clientSocket) implements Runnable {
    @Override
      public void run() {
        try {
          handleConnection(clientSocket);
        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        }
      }
    }
}
