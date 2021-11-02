package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	/**
	 * A help class to facilitate organizing the information of each field
	 */
	public static class TDItem implements Serializable {

		private static final long serialVersionUID = 1L;

		/**
		 * The type of the field
		 */
		public final Type fieldType;

		/**
		 * The name of the field
		 */
		public final String fieldName;

		public TDItem(Type t, String n) {
			this.fieldName = n;
			this.fieldType = t;
		}

		public String toString() {
			return fieldName + "(" + fieldType + ")";
		}
	}

	/**
	 * @return An iterator which iterates over all the field TDItems that are
	 *         included in this TupleDesc
	 */
	public Iterator<TDItem> iterator() {
		// some code goes here
		if (tupleDesc != null)
			return new Iterator<TDItem>() {
				private int nextSlot = 0;

				public boolean hasNext() {
					if (nextSlot >= tupleDesc.length)
						return false;
					return true;
				}

				public TDItem next() {
					if (!hasNext()) {
						throw new NoSuchElementException();
					}
					return tupleDesc[nextSlot++];
				}

				public void remove() {
					throw new UnsupportedOperationException("[INFO] removal is not allowed");
				}
			};
		return null;
	}

	private static final long serialVersionUID = 1L;

	/**
	 * Create a new TupleDesc with typeAr.length fields with fields of the specified
	 * types, with associated named fields.
	 * 
	 * @param typeAr  array specifying the number of and types of fields in this
	 *                TupleDesc. It must contain at least one entry.
	 * @param fieldAr array specifying the names of the fields. Note that names may
	 *                be null.
	 */
	private TDItem[] tupleDesc = null;

	public TupleDesc(Type[] typeAr, String[] fieldAr) {
		// some code goes here
		tupleDesc = new TDItem[typeAr.length];
		for (int i = 0; i < typeAr.length; i++)
			if (fieldAr != null)
				tupleDesc[i] = new TDItem(typeAr[i], fieldAr[i] == null ? "" : fieldAr[i]);
			else
				tupleDesc[i] = new TDItem(typeAr[i], "");
	}

	/**
	 * Constructor. Create a new tuple desc with typeAr.length fields with fields of
	 * the specified types, with anonymous (unnamed) fields.
	 * 
	 * @param typeAr array specifying the number of and types of fields in this
	 *               TupleDesc. It must contain at least one entry.
	 */
	public TupleDesc(Type[] typeAr) {
		// some code goes here
		tupleDesc = new TDItem[typeAr.length];
		for (int i = 0; i < typeAr.length; i++)
			tupleDesc[i] = new TDItem(typeAr[i], "");
	}

	/**
	 * @return the number of fields in this TupleDesc
	 */
	public int numFields() {
		// some code goes here
		if (tupleDesc != null)
			return tupleDesc.length;
		return 0;
	}

	/**
	 * Gets the (possibly null) field name of the ith field of this TupleDesc.
	 * 
	 * @param i index of the field name to return. It must be a valid index.
	 * @return the name of the ith field
	 * @throws NoSuchElementException if i is not a valid field reference.
	 */
	public String getFieldName(int i) throws NoSuchElementException {
		// some code goes here

		if (tupleDesc != null) {
			if (i < 0 || i >= tupleDesc.length)
				throw new NoSuchElementException("out of index");
			return tupleDesc[i].fieldName;
		}
		return null;
	}

	/**
	 * Gets the type of the ith field of this TupleDesc.
	 * 
	 * @param i The index of the field to get the type of. It must be a valid index.
	 * @return the type of the ith field
	 * @throws NoSuchElementException if i is not a valid field reference.
	 */
	public Type getFieldType(int i) throws NoSuchElementException {
		// some code goes here
		if (tupleDesc != null) {
			if (i < 0 || i >= tupleDesc.length)
				throw new NoSuchElementException("out of index");
			return tupleDesc[i].fieldType;
		}
		return null;
	}

	/**
	 * Find the index of the field with a given name.
	 * 
	 * @param name name of the field.
	 * @return the index of the field that is first to have the given name.
	 * @throws NoSuchElementException if no field with a matching name is found.
	 */
	public int fieldNameToIndex(String name) throws NoSuchElementException {
		// some code goes here
		if (tupleDesc != null) {
			for (int i = 0; i < tupleDesc.length; i++) {
				if (tupleDesc[i].fieldName.equals(name))
					return i;
			}
			throw new NoSuchElementException("NoSuchElement name is " + name);
		} else
			throw new NoSuchElementException("tupleDesc is empty");
	}

	/**
	 * @return The size (in bytes) of tuples corresponding to this TupleDesc. Note
	 *         that tuples from a given TupleDesc are of a fixed size.
	 */
	public int getSize() {
		// some code goes here
		if (tupleDesc != null) {
			int ret = 0;
			for (int i = 0; i < tupleDesc.length; i++)
				ret += tupleDesc[i].fieldType.getLen();
			return ret;
		}
		return 0;
	}

	/**
	 * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
	 * with the first td1.numFields coming from td1 and the remaining from td2.
	 * 
	 * @param td1 The TupleDesc with the first fields of the new TupleDesc
	 * @param td2 The TupleDesc with the last fields of the TupleDesc
	 * @return the new TupleDesc
	 */
	public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
		// some code goes here
		Type[] newType = new Type[td1.numFields() + td2.numFields()];
		String[] newName = new String[td1.numFields() + td2.numFields()];
		for (int i = 0; i < td1.numFields(); i++) {
			newType[i] = td1.tupleDesc[i].fieldType;
			newName[i] = td1.tupleDesc[i].fieldName;
		}
		for (int i = 0; i < td2.numFields(); i++) {
			newType[i + td1.numFields()] = td2.tupleDesc[i].fieldType;
			newName[i + td1.numFields()] = td2.tupleDesc[i].fieldName;
		}
		return new TupleDesc(newType, newName);
	}

	/**
	 * Compares the specified object with this TupleDesc for equality. Two
	 * TupleDescs are considered equal if they have the same number of items and if
	 * the i-th type in this TupleDesc is equal to the i-th type in o for every i.
	 * 
	 * @param o the Object to be compared for equality with this TupleDesc.
	 * @return true if the object is equal to this TupleDesc.
	 */

	public boolean equals(Object o) {
		// some code goes here
		if (this.getClass().isInstance(o) && ((TupleDesc) o).numFields() == this.numFields()) {
			for (int i = 0; i < this.numFields(); i++) {
				if (!this.tupleDesc[i].fieldType.getClass().isInstance(((TupleDesc) o).tupleDesc[i].fieldType))
					return false;
			}
			return true;
		}
		return false;
	}

	public int hashCode() {
		// If you want to use TupleDesc as keys for HashMap, implement this so
		// that equal objects have equals hashCode() results
		throw new UnsupportedOperationException("unimplemented");
	}

	/**
	 * Returns a String describing this descriptor. It should be of the form
	 * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although the
	 * exact format does not matter.
	 * 
	 * @return String describing this descriptor.
	 */
	public String toString() {
		// some code goes here
		if (tupleDesc != null) {
			String ret = "";
			if (tupleDesc.length > 0)
				ret = tupleDesc[0].fieldType + "(" + tupleDesc[0].fieldName + ")";
			for (int i = 1; i < tupleDesc.length; i++)
				ret += "," + tupleDesc[i].fieldType + "(" + tupleDesc[i].fieldName + ")";
			return ret;
		}
		return "";
	}
}
