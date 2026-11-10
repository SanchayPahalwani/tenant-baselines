package io.github.sanchaypahalwani.tenantbaselines.baseline;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.IntToDoubleFunction;

/** Median and median absolute deviation read from bin counts, each bin taken at its value. */
public final class HistogramStats {

  private record Deviation(double distance, int count) {}

  private HistogramStats() {}

  private static long total(int[] counts) {
    long n = 0;
    for (int c : counts) {
      n += c;
    }
    return n;
  }

  /** The value of the bin holding the ceil(n/2)-th value, or NaN when the histogram is empty. */
  public static double median(int[] counts, IntToDoubleFunction representative) {
    long n = total(counts);
    if (n == 0) {
      return Double.NaN;
    }
    long rank = (n + 1) / 2;
    long running = 0;
    for (int bin = 0; bin < counts.length; bin++) {
      running += counts[bin];
      if (running >= rank) {
        return representative.applyAsDouble(bin);
      }
    }
    throw new IllegalStateException("rank past the last bin");
  }

  /**
   * The ceil(n/2)-th smallest distance of a value from the median, before the 1.4826 scale; NaN
   * when the histogram is empty.
   */
  public static double mad(int[] counts, IntToDoubleFunction representative, double median) {
    long n = total(counts);
    if (n == 0) {
      return Double.NaN;
    }
    List<Deviation> deviations = new ArrayList<>();
    for (int bin = 0; bin < counts.length; bin++) {
      if (counts[bin] > 0) {
        double distance = Math.abs(representative.applyAsDouble(bin) - median);
        deviations.add(new Deviation(distance, counts[bin]));
      }
    }
    deviations.sort(Comparator.comparingDouble(Deviation::distance));
    long rank = (n + 1) / 2;
    long running = 0;
    for (Deviation d : deviations) {
      running += d.count();
      if (running >= rank) {
        return d.distance();
      }
    }
    throw new IllegalStateException("rank past the last bin");
  }
}
