import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

public class RedisClient {
  private final String host;
  private final int port;
  private final Socket socket;
  private final OutputStream output;
  private final InputStream input;
  private final BufferedReader reader;

  public RedisClient(String host, int port) throws IOException {
    this.host = host;
    this.port = port;
    try {
      socket = new Socket(host, port);
      output = socket.getOutputStream();
      input = socket.getInputStream();
      reader = new BufferedReader(new InputStreamReader(input));
      System.out.println("Connecting to Redis server at " + host + ":" + port);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void send(String text) {
    try {
      //System.out.println("Sending: " + text);
      output.write(text.getBytes());
      output.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public String getReplyLine() {
    try {
      String reply = reader.readLine();
      //System.out.println("Got reply: " + reply);
      return reply;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  public void close() {
    try {
      output.close();
      input.close();
      socket.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
