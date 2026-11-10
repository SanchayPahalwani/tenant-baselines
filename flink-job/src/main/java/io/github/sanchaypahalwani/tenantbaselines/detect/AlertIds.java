package io.github.sanchaypahalwani.tenantbaselines.detect;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Deterministic alert ids, so a replayed or restarted job re-emits the same id. */
public final class AlertIds {

  private AlertIds() {}

  public static String of(
      String detector, String setting, String scope, String subject, String signal, long startsAtMs) {
    String joined =
        String.join("|", detector, setting, scope, subject, signal, Long.toString(startsAtMs));
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(joined.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest).substring(0, 32);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
