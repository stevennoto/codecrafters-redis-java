public class RdbException extends Exception {
  public RdbException(String message) {
    super(message);
  }

  public RdbException(String message, Throwable cause) {
    super(message, cause);
  }
}
