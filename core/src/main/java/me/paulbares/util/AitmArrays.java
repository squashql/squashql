package me.paulbares.util;

public final class AitmArrays {

  /**
   * Searches the specified array of ints for the specified value.
   *
   * @param a the array to be searched
   * @param key the value to be searched for
   * @return index of the search key, if it is contained in the array; otherwise, <code>- 1</code>.
   */
  public static int search(int[] a, int key) {
    for (int j = 0; j < a.length; j++) {
      if (a[j] == key) {
        return j;
      }
    }
    return -1;
  }
}
