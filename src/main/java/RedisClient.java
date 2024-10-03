import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class RedisClient {
  private final String host;
  private final int port;
  private final Socket socket;
  private final OutputStream output;
  private final InputStream input;

  public RedisClient(String host, int port) throws IOException {
    this.host = host;
    this.port = port;
    try {
      socket = new Socket(host, port);
      output = socket.getOutputStream();
      input = socket.getInputStream();
      System.out.println("Connecting to Redis server at " + host + ":" + port);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void send(String text) {
    try {
      System.out.println("Sending: " + text);
      output.write(text.getBytes());
      output.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
    // TODO: read and return response
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
