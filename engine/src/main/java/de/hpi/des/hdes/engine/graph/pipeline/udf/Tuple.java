package de.hpi.des.hdes.engine.graph.pipeline.udf;

import de.hpi.des.hdes.engine.generators.PrimitiveType;
import lombok.Getter;
import lombok.Setter;

@Getter
public class Tuple {
  final private PrimitiveType[] types;
  final private Tuple previousTuple;
  @Setter
  private Tuple nextTuple;
  private int index;
  private String transformation;
  private OperationType operation;
  private PrimitiveType type;

  public enum OperationType {
    GET, REMOVE, ADD, MUTATE
  }

  public Tuple(PrimitiveType... types) {
    this.types = types;
    previousTuple = null;
  }

  private Tuple(Tuple previous, PrimitiveType... types) {
    this.types = types;
    previousTuple = previous;
    previous.setNextTuple(this);
  }

  public boolean isFirst() {
    return this.previousTuple == null;
  }

  public boolean isLast() {
    return this.nextTuple == null;
  }

  /**
   * Gets a value at a specified index. All further operations only have that one
   * available
   * 
   * @param index The 0-based index of which item to get from the tuple
   * @return A new tuple
   */
  public Tuple get(int index) {
    if (index < 0 || index >= types.length)
      throw new Error("Tuple index out of bounds");
    this.transformation = null;
    this.index = index;
    this.operation = OperationType.GET;
    this.type = types[index];
    return new Tuple(this, types[index]);
  }

  /**
   * Appends a value to the very end of the tuple
   * 
   * @param type           The type of the added variable
   * @param transformation The string syntax of a lambda function to execute
   * @return A new tuple
   */
  public Tuple add(PrimitiveType type, String transformation) {
    return this.addAt(types.length, type, transformation);
  }

  /**
   * Adds a value a the 0-based given position, meaning if it is e.g. the value
   * will be found at the the third position in the next tuple
   * 
   * @param index          The 0-based index, where to insert the value
   * @param type           The type of the newly added variable
   * @param transformation A lambda function in String form on how to calculate
   *                       the new value
   * @return A new tuple
   */
  public Tuple addAt(int index, PrimitiveType type, String transformation) {
    if (index < 0 || index > types.length)
      throw new Error("Tuple index out of bounds");
    this.transformation = transformation;
    this.index = index;
    this.operation = OperationType.ADD;
    this.type = type;
    PrimitiveType[] newTypes = new PrimitiveType[types.length + 1];
    int typesIndex = 0;
    for (int i = 0; i < newTypes.length; i++) {
      if (i == index) {
        newTypes[i] = type;
      } else {
        newTypes[i] = types[typesIndex];
        typesIndex++;
      }
    }
    return new Tuple(this, newTypes);
  }

  /**
   * Removes a certain item from the tuple
   * 
   * @param index A 0-based index of which element to remove from the tuple
   * @return A new tuple
   */
  public Tuple remove(int index) {
    if (index < 0 || index >= types.length)
      throw new Error("Tuple index out of bounds");
    this.transformation = null;
    this.index = index;
    this.operation = OperationType.REMOVE;
    this.type = null;
    PrimitiveType[] newTypes = new PrimitiveType[types.length - 1];
    int typesIndex = 0;
    for (int i = 0; i < types.length; i++) {
      if (i != index) {
        newTypes[typesIndex] = types[i];
        typesIndex++;
      }
    }
    return new Tuple(this, newTypes);
  }

  /**
   * Mutates a value at the given index. This does leave the rest of the tuple as
   * is.
   * 
   * @param index          0-based index of the value to mutate
   * @param type           Type of the new value
   * @param transformation Lambda function in stringform to calculate new value
   * @return A new tuple
   */
  public Tuple mutateAt(int index, PrimitiveType type, String transformation) {
    if (index < 0 || index >= types.length)
      throw new Error("Tuple index out of bounds");
    this.transformation = transformation;
    this.index = index;
    this.operation = OperationType.MUTATE;
    this.type = type;
    PrimitiveType[] newTypes = new PrimitiveType[types.length];
    for (int i = 0; i < newTypes.length; i++) {
      if (i == index) {
        newTypes[i] = type;
      } else {
        newTypes[i] = types[i];
      }
    }
    return new Tuple(this, newTypes);
  }

  public Tuple getFirst() {
    Tuple t = this;
    while (!t.isFirst()) {
      t = t.getPreviousTuple();
    }
    return t;
  }

}