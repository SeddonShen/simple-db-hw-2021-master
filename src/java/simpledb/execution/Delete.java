package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    TransactionId t = null;
    OpIterator child = null;
    boolean deleteFlag = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
    	this.t = t;
    	this.child = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if(!deleteFlag)
    	{
	    	int count = 0;
	    	while(child.hasNext())
	    	{
	    		try {
	    			
					Database.getBufferPool().deleteTuple(t, child.next());
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
	    	Tuple ret =  new Tuple(getTupleDesc());
	    	ret.setField(0, new IntField(count));
	    	deleteFlag = true;
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
