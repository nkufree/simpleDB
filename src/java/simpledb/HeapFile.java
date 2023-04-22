package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	private final File file;
	private final TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.file = f;
    	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
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
       return file.getAbsoluteFile().hashCode();
       }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pgNo = pid.getPageNumber();
        try {
         RandomAccessFile raf = new RandomAccessFile(file,"r");
         int offset = BufferPool.getPageSize() * pgNo;
         byte[] data = new byte[BufferPool.getPageSize()];
         raf.seek(offset);
         raf.read(data, 0, BufferPool.getPageSize());
         raf.close();
         return new HeapPage((HeapPageId) pid, data);

        } catch (IOException e) {
               throw new IllegalArgumentException("readPage: failed to read page");

        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    	int pgNo = page.getId().getPageNumber();
        if(pgNo > numPages()){
            throw new IllegalArgumentException();
        }
        int pgSize = BufferPool.getPageSize();
        //write IO
        RandomAccessFile f = new RandomAccessFile(file,"rw");
        // set offset
        f.seek(pgNo*pgSize);
        // write
        byte[] data = page.getPageData();
        f.write(data);
        f.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.floor(file.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	ArrayList<Page> pageList= new ArrayList<Page>();
        for(int i=0;i<numPages();++i){
            // took care of getting new page
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,
                    new HeapPageId(this.getId(),i),Permissions.READ_WRITE);
            if(p.getNumEmptySlots() == 0)
                continue;
            p.insertTuple(t);
            pageList.add(p);
            return pageList;
        }
        // no new page
        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(file,true));
        byte[] emptyData = HeapPage.createEmptyPageData();
        bw.write(emptyData);
        bw.close();
        // load into cache
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,
                new HeapPageId(getId(),numPages()-1),Permissions.READ_WRITE);
        p.insertTuple(t);
        pageList.add(p);
        return pageList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    	 ArrayList<Page> pageList = new ArrayList<Page>();
         HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,
                 t.getRecordId().getPageId(),Permissions.READ_WRITE);
         p.deleteTuple(t);
         pageList.add(p);
         return pageList;

        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    	
        return new HeapFileIterator(tid,this);
    }
    
    public class HeapFileIterator implements DbFileIterator {
        private TransactionId tid;
        private HeapFile heapFile;
        private int currentPageNum;
        private Iterator<Tuple> currentTupleIterator;

        public HeapFileIterator(TransactionId tid, HeapFile hf) {
            this.tid = tid;
            this.heapFile = hf;
            this.currentPageNum = -1;
            this.currentTupleIterator = null;
        }

        public void open() throws DbException, TransactionAbortedException {
            currentPageNum = 0;
            PageId pid = new HeapPageId(heapFile.getId(), currentPageNum);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            currentTupleIterator = page.iterator();
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (currentTupleIterator == null) {
                return false;
            }
            if (currentTupleIterator.hasNext()) {
                return true;
            } else {
                while (currentPageNum < heapFile.numPages() - 1) {
                    currentPageNum++;
                    PageId pid = new HeapPageId(heapFile.getId(), currentPageNum);
                    HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                    currentTupleIterator = page.iterator();
                    if (currentTupleIterator.hasNext()) {
                        return true;
                    }
                }
            }
            return false;
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (currentTupleIterator == null || !currentTupleIterator.hasNext()) {
                throw new NoSuchElementException();
            }
            return currentTupleIterator.next();
        }

        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        public void close() {
            currentPageNum = -1;
            currentTupleIterator = null;
        }
    }

    
    

}

