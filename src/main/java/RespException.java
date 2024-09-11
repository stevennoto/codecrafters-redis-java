public class RespException extends Exception {
  public RespException(String message) {
    super(message);
  }

  public RespException(String message, Throwable cause) {
    super(message, cause);
  }
}
