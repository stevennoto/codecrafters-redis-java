public record RedisValue(String value, Long expiryTime) {

  RedisValue(String value) {
    this(value, null);
  }
}
