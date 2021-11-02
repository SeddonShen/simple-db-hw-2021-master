package simpledb.execution;

import java.io.IOException;
import java.util.NoSuchElementException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    TransactionId t = null;
    OpIterator child = null;
    int tableId = -1;
    boolean Flag = false;
    
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
    	this.t = t;
    	this.child = child;
    	this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	return new TupleDesc(new Type[] {Type.INT_TYPE});
        //return null;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	child.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	close();
    	open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if( !Flag )
    	{
	    	int count = 0;
	    	while(child.hasNext())
	    	{
	    		try {
					Database.getBufferPool().insertTuple(t, tableId, child.next());
				} catch (TransactionAbortedException e) {
					// TODO Auto-generated catch block
					throw e;
				} 
	    		catch (DbException e) {
					// TODO Auto-generated catch block
					throw e;
				} 
	    		catch (NoSuchElementException e) {
					// TODO Auto-generated catch block
	    			throw e;
				} 
	    		catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	    		count++;
	    	}
	    	Flag = true;
	    	Tuple ret =  new Tuple(new TupleDesc(new Type[] {Type.INT_TYPE}));
	    	ret.setField(0, new IntField(count));
	        return ret;
    	}
    	return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
    	return new OpIterator[] {child};
        //return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    	child = children[0];
    }
}
