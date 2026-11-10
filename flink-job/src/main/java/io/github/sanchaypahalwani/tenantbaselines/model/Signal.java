package io.github.sanchaypahalwani.tenantbaselines.model;

/** A monitored signal and the direction in which it deviates. */
public enum Signal {
  ERROR_RATE("error_rate", Direction.UP),
  LATENCY_P95("latency_p95", Direction.UP),
  VOLUME_5M("volume_5m", Direction.DOWN);

  public enum Direction {
    UP,
    DOWN
  }

  private final String wire;
  private final Direction direction;

  Signal(String wire, Direction direction) {
    this.wire = wire;
    this.direction = direction;
  }

  public String wire() {
    return wire;
  }

  public Direction direction() {
    return direction;
  }
}
