import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

  private static int PORT = 6379;

  private static final Map<RedisConfig, String> CONFIG = new HashMap<>();

  private static boolean IS_MASTER = true;
  private static String MASTER_REPLID;
  private static int MASTER_REPL_OFFSET = 0;

  public static void main(String[] args){
    if (args != null && args.length > 0) {
      initializeConfig(new LinkedList<>(Arrays.asList(args)));
    }

    if (!CONFIG.isEmpty()) {
      processConfig();
    }

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

  // Initialize config from command line args. Currently, supports dir/dbfilename, port
  private static void initializeConfig(LinkedList<String> options) {
    while (!options.isEmpty()) {
      String option = options.removeFirst().toLowerCase();
      if (!option.startsWith("--")) {
        System.out.println("Unknown option: " + option);
        continue;
      }
      RedisConfig redisConfig = RedisConfig.fromName(option.substring(2));
      if (redisConfig != null) {
        if (options.isEmpty() || options.getFirst().startsWith("--")) {
          System.out.println("Missing value for option: " + redisConfig.getName());
          continue;
        }
        String optionValue = options.removeFirst();
        CONFIG.put(redisConfig, optionValue);
        System.out.println("Setting config " + redisConfig.getName() + " to: " + optionValue);
      } else {
        System.out.println("Unknown option: " + option);
      }
    }
  }

  // Process initialized config. Currently, supports dir/dbfilename, port
  private static void processConfig() {
    if (CONFIG.containsKey(RedisConfig.DIR) && CONFIG.containsKey(RedisConfig.DBFILENAME)) {
      RdbFileParser.parseRdbFileAndLoadKeysValues(
          CONFIG.get(RedisConfig.DIR),
          CONFIG.get(RedisConfig.DBFILENAME),
          keyValueStore);
    }

    if (CONFIG.containsKey(RedisConfig.PORT)) {
      try {
        PORT = Integer.parseInt(CONFIG.get(RedisConfig.PORT));
      } catch (NumberFormatException e) {
        System.out.println("Invalid port number: " + CONFIG.get(RedisConfig.PORT));
      }
      System.out.println("Setting port to: " + CONFIG.get(RedisConfig.PORT));
    }

    if (CONFIG.containsKey(RedisConfig.REPLICA_OF)) {
      try {
        // Connect to master
        String[] replicaOfHostPort = CONFIG.get(RedisConfig.REPLICA_OF).split(" ");
        String replicaOfHost = replicaOfHostPort[0];
        int replicaOfPort = Integer.parseInt(replicaOfHostPort[1]);
        RedisClient redisClient = new RedisClient(replicaOfHost, replicaOfPort);
        // send PING
        redisClient.send(RespUtil.serializeArray(List.of("PING")));
        if (!redisClient.getReplyLine().equals("+PONG")) {
          throw new RuntimeException("Invalid PING reply from master");
        }
        // send REPLCONF with port
        redisClient.send(RespUtil.serializeArray(List.of("REPLCONF", "listening-port", String.valueOf(PORT))));
        if (!redisClient.getReplyLine().equals("+OK")) {
          throw new RuntimeException("Invalid REPLCONF reply from master");
        }
        // send REPLCONF with (hardcoded for now) capabilities
        redisClient.send(RespUtil.serializeArray(List.of("REPLCONF", "capa", "psync2")));
        if (!redisClient.getReplyLine().equals("+OK")) {
          throw new RuntimeException("Invalid REPLCONF reply from master");
        }
        // send PSYNC to ask for replication id and offset
        redisClient.send(RespUtil.serializeArray(List.of("PSYNC", "?", "-1")));
        if (!redisClient.getReplyLine().startsWith("+FULLRESYNC")) { // TODO handle repl id and offset
          throw new RuntimeException("Invalid PSYNC reply from master");
        }
        IS_MASTER = false;
        System.out.println("Set as slave replica of: " + replicaOfHost + ":" + replicaOfPort);
      } catch (Exception e) {
        System.out.println("Error setting as slave replica of: " + CONFIG.get(RedisConfig.REPLICA_OF));
        e.printStackTrace();
      }
    }
    MASTER_REPLID = generateRandomAlphaNumericString(40);
    MASTER_REPL_OFFSET = 0;
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

  private static final Map<String, RedisValue> keyValueStore = new HashMap<>();

  // Process simple commands. Currently supports:
  // - PING
  // - ECHO <value>
  // - CONFIG GET <config-name> // only one at a time so far
  // - SET <key> <value> [PX <expiry in ms>]
  // - GET <key>
  private static String processSimpleCommand(List<String> commandArray) {
    String command = commandArray.getFirst().toUpperCase();
    switch (command) {
      case "PING":
        return RespUtil.serializeSimpleString("PONG");
      case "ECHO":
        String payload = commandArray.get(1);
        return RespUtil.serializeBulkString(payload);
      case "CONFIG":
        String getString = commandArray.get(1);
        RedisConfig configToGet = RedisConfig.fromName(commandArray.get(2));
        if (!getString.equalsIgnoreCase("get") || configToGet == null) {
          return RespUtil.serializeBulkString(null);
        }
        return RespUtil.serializeArray(Arrays.asList(configToGet.getName(), CONFIG.get(RedisConfig.DIR)));
      case "INFO":
        // TODO: for now only supporting `INFO replication`
        return RespUtil.serializeBulkString(
            "# Replication\n"
                + "role:" + (IS_MASTER ? "master" : "slave") + "\n"
                + "master_replid:" + MASTER_REPLID + "\n"
                + "master_repl_offset:" + MASTER_REPL_OFFSET
        );
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
        if (valueToGet == null) {
          return RespUtil.serializeBulkString(null);
        }
        if (valueToGet.expiryTime() != null && valueToGet.expiryTime() < System.currentTimeMillis()) {
          // TODO: expire eagerly instead of lazily on GET?
          System.out.println("Expiring key: " + keyToGet + " with expiryTime: " + valueToGet.expiryTime());
          keyValueStore.remove(keyToGet);
          return RespUtil.serializeBulkString(null);
        }
        return RespUtil.serializeBulkString(valueToGet.value());
      case "KEYS":
        String keysPattern = commandArray.get(1);
        if (keysPattern.equals("*")) {
          return RespUtil.serializeArray(new ArrayList<>(keyValueStore.keySet()));
        } else {
          // TODO handle pattern matching
          return RespUtil.serializeBulkString(null);
        }
      case "REPLCONF":
        String replyConfKey = commandArray.get(1);
        String replyConfValue = commandArray.get(2);
        System.out.println("Received REPLCONF: " + replyConfKey + " " + replyConfValue);
        // TODO: use these values
        return RespUtil.serializeBulkString("OK");
      case "PSYNC":
        String psyncReplId = commandArray.get(1);
        String psyncReplOffset = commandArray.get(2);
        System.out.println("Received PSYNC: " + psyncReplId + " " + psyncReplOffset);
        // TODO: use these values
        return RespUtil.serializeBulkString("FULLRESYNC " + MASTER_REPLID + " " + MASTER_REPL_OFFSET);
      default:
        return "";
    }
  }

  private static String generateRandomAlphaNumericString(int length) {
    String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    StringBuilder text = new StringBuilder();
    Random rand = new Random();
    while (text.length() < length) {
      text.append(chars.charAt((int) (rand.nextFloat() * chars.length())));
    }
    return text.toString();
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
