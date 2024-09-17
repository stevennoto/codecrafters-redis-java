public enum RedisConfig {
  DIR("dir"),
  DBFILENAME("dbfilename");

  private final String name;

  RedisConfig(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static RedisConfig fromName(String name) {
    for (RedisConfig config : RedisConfig.values()) {
      if (config.getName().equalsIgnoreCase(name)) {
        return config;
      }
    }
    return null;
  }
}
