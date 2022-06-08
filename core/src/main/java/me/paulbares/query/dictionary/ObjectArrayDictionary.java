package me.paulbares.query.dictionary;

import org.eclipse.collections.api.block.HashingStrategy;
import org.eclipse.collections.api.block.procedure.primitive.ObjectIntProcedure;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMapWithHashingStrategy;

import java.util.Arrays;

public class ObjectArrayDictionary {

  /**
   * The value to indicate no value
   */
  private static final int FREE = -1;

  protected final ObjectIntHashMapWithHashingStrategy<Object[]> underlyingDic;
  protected final int pointLength;

  public ObjectArrayDictionary(int pointLength) {
    this(pointLength, 16);
  }

  public ObjectArrayDictionary(int pointLength, int initialCapacity) {
    this.underlyingDic = new ObjectIntHashMapWithHashingStrategy<>(ObjectArrayHashingStrategy.INSTANCE, initialCapacity);
    this.pointLength = pointLength;
  }

  /**
   * The length of the array should be the same as {@link #pointLength}. No check is made.
   *
   * @param value
   * @return
   */
  public int map(Object[] value) {
    assert value.length == this.pointLength;
    int size = this.underlyingDic.size();
    return this.underlyingDic.getIfAbsentPut(value, size);
  }

  /**
   * Gets the position of the key in the dictionary.
   *
   * @param key the key to find
   * @return position of the key in the dictionary, or -1 if the key is not in the dictionary.
   */
  public int getPosition(int[] key) {
    return this.underlyingDic.getIfAbsent(key, FREE);
  }

  public int getPointLength() {
    return this.pointLength;
  }

  public int size() {
    return this.underlyingDic.size();
  }

  public void forEach(ObjectIntProcedure<Object[]> procedure) {
    this.underlyingDic.forEachKeyValue(procedure);
  }

  private static final class ObjectArrayHashingStrategy implements HashingStrategy<Object[]> {
    private static final long serialVersionUID = 1L;

    private static HashingStrategy<Object[]> INSTANCE = new ObjectArrayHashingStrategy();

    @Override
    public int computeHashCode(Object[] object) {
      return Arrays.hashCode(object); // FIXME poor hash function
    }

    @Override
    public boolean equals(Object[] object1, Object[] object2) {
      return Arrays.equals(object1, object2);
    }
  }
}
