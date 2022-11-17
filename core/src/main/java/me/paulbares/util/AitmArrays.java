package me.paulbares.util;

public final class AitmArrays {

  public static int search(int[] array, int i) {
    return java.util.Arrays.stream(array).filter(aa -> aa == i).findFirst().orElse(-1);
  }
}
