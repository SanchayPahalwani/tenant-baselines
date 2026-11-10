package io.github.sanchaypahalwani.tenantbaselines;

import io.github.sanchaypahalwani.tenantbaselines.detect.SeasonalSetting;
import java.util.ArrayList;
import java.util.List;

/**
 * Command-line settings for the job. {@code seasonal} holds one setting per {@code --seasonal-k}
 * value; {@code emitScores} says whether scores are written, by default only in the live
 * namespace.
 */
public record JobConfig(
    String bootstrap,
    String ns,
    String runId,
    String mode,
    String thresholdsPath,
    long replayEndMs,
    boolean bounded,
    String groupId,
    List<SeasonalSetting> seasonal,
    boolean emitScores) {

  public static JobConfig parse(String[] args) {
    String bootstrap = "kafka:19092";
    String ns = "live";
    String runId = null;
    String mode = "live";
    String thresholds = "/opt/tb-config/thresholds-demo.json";
    long replayEndMs = Long.MIN_VALUE;
    boolean bounded = false;
    String groupId = null;
    String seasonalK = "4";
    Boolean emitScores = null;

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
        case "--seasonal-k" -> seasonalK = value(args, ++i);
        case "--scores" -> emitScores = bool("--scores", value(args, ++i));
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
        groupId != null ? groupId : "tb-" + ns,
        seasonal(seasonalK),
        emitScores != null ? emitScores : ns.equals("live"));
  }

  private static String value(String[] args, int i) {
    if (i >= args.length) {
      throw new IllegalArgumentException("Missing value for " + args[i - 1]);
    }
    return args[i];
  }

  private static boolean bool(String flag, String v) {
    return switch (v) {
      case "true" -> true;
      case "false" -> false;
      default -> throw new IllegalArgumentException(flag + " must be true or false, got " + v);
    };
  }

  /**
   * One setting per comma-separated k, in the order given. Two settings with one name would step
   * the same alert machines twice a minute, so a repeated k is rejected.
   */
  private static List<SeasonalSetting> seasonal(String ks) {
    List<SeasonalSetting> settings = new ArrayList<>();
    for (String part : ks.split(",")) {
      double k = Double.parseDouble(part.trim());
      if (!(k > 0) || Double.isInfinite(k)) {
        throw new IllegalArgumentException("--seasonal-k values must be positive, got " + part);
      }
      SeasonalSetting s = SeasonalSetting.ofK(k);
      if (settings.stream().anyMatch(o -> o.setting().equals(s.setting()))) {
        throw new IllegalArgumentException("--seasonal-k repeats " + part);
      }
      settings.add(s);
    }
    return List.copyOf(settings);
  }

  public String eventsTopic() {
    return ns + ".events";
  }

  public String aggregatesTopic() {
    return ns + ".aggregates";
  }

  public String scoresTopic() {
    return ns + ".scores";
  }

  public String alertsTopic() {
    return ns + ".alerts";
  }
}
