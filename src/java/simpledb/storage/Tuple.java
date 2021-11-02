package simpledb.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * Create a new tuple with the specified schema (type).
	 *
	 * @param td the schema of this tuple. It must be a valid TupleDesc instance
	 *           with at least one field.
	 */
	private Field[] tupleField = null;
	private TupleDesc tupleDesc = null;
	private RecordId recordId = null;

	public Tuple(TupleDesc td) {
		// some code goes here
		tupleDesc = td;
		tupleField = new Field[td.numFields()];
	}

	/**
	 * @return The TupleDesc representing the schema of this tuple.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		return tupleDesc;
	}

	/**
	 * @return The RecordId representing the location of this tuple on disk. May be
	 *         null.
	 */
	public RecordId getRecordId() {
		// some code goes here
		return recordId;
	}

	/**
	 * Set the RecordId information for this tuple.
	 *
	 * @param rid the new RecordId for this tuple.
	 */
	public void setRecordId(RecordId rid) {
		// some code goes here
		recordId = rid;
	}

	/**
	 * Change the value of the ith field of this tuple.
	 *
	 * @param i index of the field to change. It must be a valid index.
	 * @param f new value for the field.
	 * @throws Exception
	 */
	public void setField(int i, Field f) {
		// some code goes here
		assert (i >= 0 && i < tupleField.length);
		tupleField[i] = f;
	}

	/**
	 * @return the value of the ith field, or null if it has not been set.
	 *
	 * @param i field index to return. Must be a valid index.
	 * @throws Exception
	 */
	public Field getField(int i) {
		// some code goes here
		assert (i >= 0 && i < tupleField.length);
		return tupleField[i];
	}

	/**
	 * Returns the contents of this Tuple as a string. Note that to pass the system
	 * tests, the format needs to be as follows:
	 *
	 * column1\tcolumn2\tcolumn3\t...\tcolumnN
	 *
	 * where \t is any whitespace (except a newline)
	 */
	public String toString() {
		// some code goes here
		String ret = "";
		if (tupleField.length > 0)
			ret = tupleField[0].toString();
		for (int i = 1; i < tupleField.length; i++)
			ret += "\t" + tupleField[i].toString();
		return ret;
		// throw new UnsupportedOperationException("Implement this");
	}

	/**
	 * @return An iterator which iterates over all the fields of this tuple
	 */
	public Iterator<Field> fields() {
		// some code goes here
		if (tupleField != null)
			return new Iterator<Field>() {
				private int nextSlot = 0;

				public boolean hasNext() {
					if (nextSlot >= tupleField.length)
						return false;
					return true;
				}

				public Field next() {
					if (!hasNext()) {
						throw new NoSuchElementException();
					}
					return tupleField[nextSlot++];
				}
			};
		return null;
	}

	/**
	 * reset the TupleDesc of this tuple (only affecting the TupleDesc)
	 */
	public void resetTupleDesc(TupleDesc td) {
		// some code goes here
		tupleDesc = td;
	}
}
