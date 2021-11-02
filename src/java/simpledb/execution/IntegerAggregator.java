package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    int gbfield = -1;
    Type gbfieldtype = null;
    int afield = -1;
    Op what = null;
    
    
    Object value = null;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	if (this.gbfield == Aggregator.NO_GROUPING) 
    		value = (Object) new ArrayList<Integer>();
        else {
            assert gbfieldtype != null;
            if (gbfieldtype == Type.INT_TYPE) 
            	value = (Object) new HashMap<Integer, ArrayList<Integer>>();
            else 
            	value = (Object) new HashMap<String, ArrayList<Integer>>();
            
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	if (this.gbfield == Aggregator.NO_GROUPING) {
            ((ArrayList<Integer>) value).add(((IntField)tup.getField(afield)).getValue());
        } else {
            if (gbfieldtype == Type.INT_TYPE) {
            	HashMap<Integer, ArrayList<Integer>> groupAggr = (HashMap<Integer, ArrayList<Integer>>) value;
                Integer gbKey = ((IntField) tup.getField(gbfield)).getValue();
                Integer aggrVal = ((IntField) tup.getField(afield)).getValue();

                if (!groupAggr.containsKey(gbKey)) {
                    groupAggr.put(gbKey, new ArrayList<>(1));
                }
                groupAggr.get(gbKey).add(aggrVal);
            } else if (gbfieldtype == Type.STRING_TYPE) {
            	HashMap<String, ArrayList<Integer>> groupAggr = (HashMap<String, ArrayList<Integer>>) value;
                String gbKey = ((StringField) tup.getField(gbfield)).getValue();
                Integer aggrVal = ((IntField) tup.getField(afield)).getValue();

                if (!groupAggr.containsKey(gbKey)) {
                    groupAggr.put(gbKey, new ArrayList<>(1));
                }
                groupAggr.get(gbKey).add(aggrVal);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
    	return new AggrDbIterator();
    }

    private class AggrDbIterator implements OpIterator {
        private ArrayList<Tuple> res;
        private Iterator<Tuple> it;

        public int calcAggrRes(ArrayList<Integer> l) {
            assert !l.isEmpty();
            int res = 0;
            switch (what) {
                case MIN:
                    res = l.get(0);
                    for (int v : l) {
                        if (res > v) {
                            res = v;
                        }
                    }
                    break;
                case MAX:
                    res = l.get(0);
                    for (int v : l) {
                        if (res < v) {
                            res = v;
                        }
                    }
                    break;
                case SUM:
                    res = 0;
                    for (int v : l) {
                        res += v;
                    }
                    break;
                case AVG:
                    res = 0;
                    for (int v : l) {
                        res += v;
                    }
                    res = res / l.size();
                    break;
                case COUNT:
                    res = l.size();
                    break;
                case SUM_COUNT:
                    throw new NoSuchElementException();
                case SC_AVG:
                    throw new NoSuchElementException();
            }
            return res;
        }

        public AggrDbIterator() {
            res = new ArrayList<Tuple>();
            if (gbfield == Aggregator.NO_GROUPING) {
                Tuple t = new Tuple(getTupleDesc());
                Field aggregateVal = new IntField(this.calcAggrRes((ArrayList<Integer>) value));
                t.setField(0, aggregateVal);
                res.add(t);
            } else {
                for (Map.Entry e : ((HashMap<Integer, ArrayList<Integer>>) value).entrySet()) {
                    Tuple t = new Tuple(getTupleDesc());
                    Field groupVal = null;
                    if (gbfieldtype == Type.INT_TYPE) {
                        groupVal = new IntField((int) e.getKey());
                    } else {
                        String str = (String) e.getKey();
                        groupVal = new StringField(str, str.length());
                    }
                    Field aggregateVal = new IntField(this.calcAggrRes((ArrayList<Integer>) e.getValue()));
                    t.setField(0, groupVal);
                    t.setField(1, aggregateVal);
                    res.add(t);
                }
            }
        }

        /**
         * Opens the iterator. This must be called before any of the other methods.
         *
         * @throws DbException when there are problems opening/accessing the database.
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            it = res.iterator();
        }

        /**
         * Returns true if the iterator has more tuples.
         *
         * @return true f the iterator has more tuples.
         * @throws IllegalStateException If the iterator has not been opened
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (it == null) {
                throw new IllegalStateException("IntegerAggregator not open");

            }
            return it.hasNext();

        }

        /**
         * Returns the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return the next tuple in the iteration.
         * @throws NoSuchElementException if there are no more tuples.
         * @throws IllegalStateException  If the iterator has not been opened
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it == null) {
                throw new IllegalStateException("IntegerAggregator not open");
            }
            return it.next();
        }

        /**
         * Resets the iterator to the start.
         *
         * @throws DbException           when rewind is unsupported.
         * @throws IllegalStateException If the iterator has not been opened
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (it == null) {
                throw new IllegalStateException("IntegerAggregator not open");
            }
            it = res.iterator();
        }

        /**
         * Returns the TupleDesc associated with this DbIterator.
         *
         * @return the TupleDesc associated with this DbIterator.
         */
        @Override
        public TupleDesc getTupleDesc() {
            if (gbfield == Aggregator.NO_GROUPING) {
                return new TupleDesc(new Type[]{Type.INT_TYPE});
            } else {
                return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            }
        }

        /**
         * Closes the iterator. When the iterator is closed, calling next(),
         * hasNext(), or rewind() should fail by throwing IllegalStateException.
         */
        @Override
        public void close() {
            it = null;
        }
    }

}
