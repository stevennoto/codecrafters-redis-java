import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RespUtil {
  // Read a simple command array from socket reader
  // Examples:
  // *1\r\n$4\r\nPING\r\n -> ["PING"]
  // *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n -> ["ECHO", "hey"]
  public static List<String> readArray(BufferedReader reader) throws RespException {
    try {
      String line = reader.readLine();
      if (line.charAt(0) != '*') {
        throw new RespException("Invalid array: no * length detected");
      }
      int length = Integer.parseInt(line.substring(1));
      //System.out.println("Got array of len: " + length);
      List<String> entries = new ArrayList<>();
      for (int i = 0; i < length; i++) {
        //System.out.println("Trying to get entry #" + i);
        entries.add(readBulkString(reader));
      }
      return entries;
    } catch (RespException | NumberFormatException e) {
      throw new RespException("Invalid array", e);
    } catch (IOException e) {
      throw new RespException("Error reading array", e);
    }
  }

  // Read a bulk string from socket reader
  // Example: $4\r\nPING -> "PING"
  public static String readBulkString(BufferedReader reader) throws RespException {
    try {
      String line = reader.readLine();
      if (line.charAt(0) != '$') {
        throw new RespException("Invalid bulk string length");
      }
      int length = Integer.parseInt(line.substring(1));
      String text = reader.readLine();
      if (text.length() != length) {
        throw new RespException(String.format("Bulk string %s with wrong length %d", text, length));
      }
      System.out.println("Got bulk string: " + text);
      return text;
    } catch (NumberFormatException | IOException e) {
      throw new RespException("Invalid bulk string", e);
    }
  }

  public static String serializeSimpleString(String text) {
    return "+" + (text == null ? "" : text) + "\r\n";
  }

  public static String serializeBulkString(String text) {
    if (text == null) {
      return "$-1\r\n";
    } else {
      return "$" + text.length() + "\r\n" + text + "\r\n";
    }
  }

  public static String serializeArray(List<String> entries) {
    StringBuilder sb = new StringBuilder();
    sb.append("*").append(entries.size()).append("\r\n");
    for (String entry : entries) {
      sb.append(serializeBulkString(entry));
    }
    return sb.toString();
  }

}
