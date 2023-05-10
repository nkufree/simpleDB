package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *  
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private final int numPages;
    private final ConcurrentHashMap<PageId,Page> pageStore;  
    private PageLockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
    	this.numPages = numPages;
    	pageStore = new ConcurrentHashMap<PageId,Page>();
    	lockManager = new PageLockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page 
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
    	//如果只读，可以获得shared锁，如果还需要修改，获得exclusive锁
    	int lockType;
        if(perm == Permissions.READ_ONLY){
            lockType = 0;
        }else{
            lockType = 1;
        }
        boolean lockAcquired = false;
    	
    	if(!pageStore.containsKey(pid)){
    		if(pageStore.size()>numPages){
                evictPage();
            }
    		
            DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbfile.readPage(pid);
            pageStore.put(pid,page);
        }
    	while(!lockAcquired)
    	{
				lockAcquired = lockManager.acquireLock(pid, tid, lockType);
    	}
    	
        return pageStore.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    	lockManager.releaseLock(pid,tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
    	return lockManager.holdsLock(p,tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	if(commit){
            flushPages(tid);
        }else{
        	for (PageId pid : pageStore.keySet()) {
                Page page = pageStore.get(pid);
     
                if (page.isDirty() == tid) {
                    int tabId = pid.getTableId();
                    DbFile file =  Database.getCatalog().getDatabaseFile(tabId);
                    Page pageFromDisk = file.readPage(pid);
                    pageStore.put(pid, pageFromDisk);
                }
            }
        }
 
        for(PageId pid:pageStore.keySet()){
            if(holdsLock(tid,pid))
                releasePage(tid,pid);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	DbFile dbfile=Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> tempArraylist=dbfile.insertTuple(tid,t);
        for (Page nowPage:tempArraylist) {
            //System.out.println(nowPage.getId()+"insert1");
            nowPage.markDirty(true,tid);
            pageStore.put(nowPage.getId(),nowPage);
        }
    }
    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	DbFile dbfile=Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> tempArraylist=dbfile.deleteTuple(tid,t);
        for(Page nowPage:tempArraylist){
            //System.out.println(nowPage.getId()+"delete1");
            nowPage.markDirty(true,tid);
            pageStore.put(nowPage.getId(),nowPage);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
    	for(Page p : this.pageStore.values())
    		flushPage(p.getId());
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    	pageStore.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    	Page p = pageStore.get(pid);
        TransactionId tid = null;
        // flush it if it is dirty
        if((tid = p.isDirty())!= null){
            Database.getLogFile().logWrite(tid,p.getBeforeImage(),p);
            Database.getLogFile().force();
            // write to disk
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(p);
            p.markDirty(false,null);
        }

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	for (Page page : pageStore.values()) {
        	if (page.isDirty() !=null && page.isDirty()==tid) {
        		flushPage(page.getId());
        	}
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	 PageId pid = new ArrayList<>(pageStore.keySet()).get(0);
         try{
             flushPage(pid);
         }catch(IOException e){
             e.printStackTrace();
         }
         discardPage(pid);

    }
    
    private class Lock{
        TransactionId tid;
        int lockType;   // 0 : shared lock , 1 : exclusive lock
 
        public Lock(TransactionId tid,int lockType){
            this.tid = tid;
            this.lockType = lockType;
        }
    }
 
    private class PageLockManager{
        ConcurrentHashMap<PageId,Vector<Lock>> lockMap;
 
        public PageLockManager(){
            lockMap = new ConcurrentHashMap<PageId,Vector<Lock>>();
        }
 
        public synchronized boolean acquireLock(PageId pid,TransactionId tid,int lockType){
            // 如果没有锁，初始化
            if(lockMap.get(pid) == null){
                Lock lock = new Lock(tid,lockType);
                Vector<Lock> locks = new Vector<>();
                locks.add(lock);
                lockMap.put(pid,locks);
 
                return true;
            }
 
            // 如果有锁，获得他们
            Vector<Lock> locks = lockMap.get(pid);
 
            // 判断该事务是否已经获得锁
            for(Lock lock:locks){
                if(lock.tid == tid){
                    // 已经获得该类型的锁
                    if(lock.lockType == lockType)
                        return true;
                    //如果已经获得exclusive锁
                    if(lock.lockType == 1)
                        return true;
                    // 如果获得的是shared锁，但是想获得exclusive锁
                    //只有在只有该页被他一个锁住时才能变为exclusive锁，否则获取锁失败
                    if(locks.size()==1){
                        lock.lockType = 1;
                        return true;
                    }else{
                        return false;
                    }
                }
            }
 
            // 如果该事务没有获得锁，判断获得锁的事务中是否有exclusive锁
            //如果有exclusive锁，那么就应该只有这一个锁
            if (locks.get(0).lockType ==1){
                assert locks.size() == 1 : "exclusive lock can't coexist with other locks";
                return false;
            }
 
            // 如果没有exclusive锁，可以赋予shared锁
            if(lockType == 0){
                Lock lock = new Lock(tid,0);
                locks.add(lock);
                lockMap.put(pid,locks);
 
                return true;
            }
            //当有shared锁时无法获得exclusive锁
            return false;
        }
 
 
        public synchronized boolean releaseLock(PageId pid,TransactionId tid){
            // 如果没有锁
            assert lockMap.get(pid) != null : "page not locked!";
            Vector<Lock> locks = lockMap.get(pid);
 
            for(int i=0;i<locks.size();++i){
                Lock lock = locks.get(i);
 
                // 释放锁
                if(lock.tid == tid){
                    locks.remove(lock);
 
                    // 如果锁释放完了，该页就会从lockMap中移除
                    if(locks.size() == 0)
                        lockMap.remove(pid);
                    return true;
                }
            }
            //该事务没有锁住该页
            return false;
        }
 
 
        public synchronized boolean holdsLock(PageId pid,TransactionId tid){
            // 该页没有锁
            if(lockMap.get(pid) == null)
                return false;
            Vector<Lock> locks = lockMap.get(pid);
 
            //判断该页中的锁是否有该事务
            for(Lock lock:locks){
                if(lock.tid == tid){
                    return true;
                }
            }
            return false;
        }
    }


}
