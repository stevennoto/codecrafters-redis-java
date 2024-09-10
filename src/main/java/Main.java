import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

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
      handleConnection(serverSocket.accept());
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }

  public static void handleConnection(Socket clientSocket) throws IOException {
    try (clientSocket) {
      BufferedReader reader = new BufferedReader(
          new InputStreamReader(clientSocket.getInputStream()));
      BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(clientSocket.getOutputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println("Received line: " + line);
        if (line.toUpperCase().contains("PING")) {
          System.out.println("Sending line: PONG");
          writer.write("+PONG\r\n");
          writer.flush();
        }
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }
}
