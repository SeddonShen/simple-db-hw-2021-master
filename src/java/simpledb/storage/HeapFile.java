package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
	
	private File f = null;
	private TupleDesc td = null;
    public HeapFile(File f, TupleDesc td) {
    	this.f = f;
    	this.td = td;
        // some code goes here
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
    	return f;
        //return null;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    	return ("ame"+f.getAbsoluteFile()).hashCode();
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	return td;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	int tableid = pid.getTableId();
        int pgNo = pid.getPageNumber();
        
        final int pageSize = Database.getBufferPool().getPageSize();
        //System.out.println("pageSize:"+pageSize+"(int) f.length():"+(int) f.length());
        byte[] rawPgData = HeapPage.createEmptyPageData();

        // random access read from disk
        try {
            FileInputStream in = new FileInputStream(f);
            in.skip(pgNo * pageSize);
            in.read(rawPgData);
            in.close();
            return new HeapPage(new HeapPageId(tableid, pgNo), rawPgData);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("FileNotFoundException:"+e.toString());
        } catch (IOException e) {
            throw new IllegalArgumentException("IOException:"+e.toString());
        }
        //return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        int tableid = pid.getTableId();
        int pgNo = pid.getPageNumber();

        final int pageSize = Database.getBufferPool().getPageSize();
        byte[] pgData = page.getPageData();

        RandomAccessFile file = new RandomAccessFile(f, "rws");
        file.skipBytes(pgNo * pageSize);
        file.write(pgData);
        file.close();
        System.out.println("i'm write file!");
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
    	int fileSizeinByte = (int) f.length();
        return fileSizeinByte / Database.getBufferPool().getPageSize();
        //return 0;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	ArrayList<Page> affected = new ArrayList<>(1);
        int numPages = numPages();

        for (int pgNo = 0; pgNo < numPages + 1; pgNo++) {
            HeapPageId pid = new HeapPageId(getId(), pgNo);
            HeapPage pg;
            if (pgNo < numPages) {
                pg = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            } else {
                // pgNo = numpages -> we need add new page
                pg = new HeapPage(pid, HeapPage.createEmptyPageData());
            }

            if (pg.getNumEmptySlots() > 0) {
                // insert will update tuple when inserted
                pg.insertTuple(t);
                // writePage(pg);
                if (pgNo < numPages) {
                    affected.add(pg);
                } else {
                    // should append the dbfile
                    writePage(pg);
                }
                return affected;
            }

        }
        // otherwise create new page and insert
        throw new DbException("HeapFile: InsertTuple: Tuple can not be added");
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    	RecordId rid = t.getRecordId();
        HeapPageId pid = (HeapPageId) rid.getPageId();
        if (pid.getTableId() == getId()) {
            HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            pg.deleteTuple(t);
            ArrayList<Page> ret = new ArrayList<Page>();
            ret.add(pg);
            return ret;
        }
        throw new DbException("HeapFile: deleteTuple: tuple.tableid != getId");
    }

    
    private class HeapFileIterator implements DbFileIterator {

        private Integer pgCursor;
        private Iterator<Tuple> tupleIter;
        private final TransactionId transactionId;
        private final int tableId;
        private final int numPages;

        public HeapFileIterator(TransactionId tid) {
            this.pgCursor = null;
            this.tupleIter = null;
            this.transactionId = tid;
            this.tableId = getId();
            this.numPages = numPages();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pgCursor = 0;
            tupleIter = getTupleIter(pgCursor);
            /*
            while( pgCursor < numPages && !getTupleIter(pgCursor).hasNext() )
            	pgCursor++;
            if( pgCursor < numPages && getTupleIter(pgCursor).hasNext() )
            	tupleIter = getTupleIter(pgCursor);
            else
            	pgCursor = null;
            */
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (pgCursor != null) {
            	if( tupleIter.hasNext() )
            		return true;
            	else
            	{
            		pgCursor++;
            		while( pgCursor < numPages && !getTupleIter(pgCursor).hasNext() )
	                 	pgCursor++;
            		if( pgCursor < numPages && getTupleIter(pgCursor).hasNext() )
            		{
                    	tupleIter = getTupleIter(pgCursor);
                    	return true;
            		}
                    else
                    {
                    	pgCursor = null;
                    	return false;
                    }
            	}
            } else {
                return false;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext())  {
                return tupleIter.next();
            }
            throw new NoSuchElementException("HeapFileIterator: error: next: no more elemens");
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            pgCursor = null;
            tupleIter = null;
        }

        private Iterator<Tuple> getTupleIter(int pgNo)
                throws TransactionAbortedException, DbException {
            PageId pid = new HeapPageId(tableId, pgNo);
            return ((HeapPage)
                    Database
                            .getBufferPool()
                            .getPage(transactionId, pid, Permissions.READ_ONLY))
                    .iterator();
        }
    }
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    	
        return new HeapFileIterator(tid);
    }

}

