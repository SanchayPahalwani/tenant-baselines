package io.github.sanchaypahalwani.tenantbaselines;

/** Command-line settings for the job. */
public record JobConfig(
    String bootstrap,
    String ns,
    String runId,
    String mode,
    String thresholdsPath,
    long replayEndMs,
    boolean bounded,
    String groupId) {

  public static JobConfig parse(String[] args) {
    String bootstrap = "kafka:19092";
    String ns = "live";
    String runId = null;
    String mode = "live";
    String thresholds = "/opt/tb-config/thresholds-demo.json";
    long replayEndMs = Long.MIN_VALUE;
    boolean bounded = false;
    String groupId = null;

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--bootstrap" -> bootstrap = value(args, ++i);
        case "--ns" -> ns = value(args, ++i);
        case "--run-id" -> runId = value(args, ++i);
        case "--mode" -> mode = value(args, ++i);
        case "--thresholds" -> thresholds = value(args, ++i);
        case "--replay-end-ms" -> replayEndMs = Long.parseLong(value(args, ++i));
        case "--bounded" -> bounded = true;
        case "--group-id" -> groupId = value(args, ++i);
        default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
      }
    }
    if (!mode.equals("live") && !mode.equals("replay")) {
      throw new IllegalArgumentException("--mode must be live or replay, got " + mode);
    }
    return new JobConfig(
        bootstrap,
        ns,
        runId != null ? runId : ns,
        mode,
        thresholds,
        replayEndMs,
        bounded,
        groupId != null ? groupId : "tb-" + ns);
  }

  private static String value(String[] args, int i) {
    if (i >= args.length) {
      throw new IllegalArgumentException("Missing value for " + args[i - 1]);
    }
    return args[i];
  }

  public String eventsTopic() {
    return ns + ".events";
  }

  public String aggregatesTopic() {
    return ns + ".aggregates";
  }

  public String alertsTopic() {
    return ns + ".alerts";
  }
}
