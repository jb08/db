package simpledb;

import java.io.*;
import java.util.*;

import com.sun.jndi.url.iiopname.iiopnameURLContextFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

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

	public class HeapFileIterator implements DbFileIterator{

		int curPage;
		int pageSize;
		int numPages;
		TransactionId tid;
		int fileID;
		Iterator<Tuple> i = null;

		HeapFileIterator(int pageSize, int numPages, TransactionId tid, int fileID) throws DbException, TransactionAbortedException{

			this.curPage = 0; 
			this.pageSize = pageSize;
			this.numPages = numPages;
			this.tid = tid; 
			this.fileID = fileID;
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			//System.out.println("HeapFileIterator hasNext()");

			// If not open
			if (this.i == null) {
				return false;
			}

			//If there are tuples left in the current page
			if(i.hasNext()){
				return true;
			}

			//If not, search through all the future pages
			for (int pageIndex = this.curPage+1; pageIndex < this.numPages; pageIndex++){

				PageId pid = new HeapPageId(this.fileID, pageIndex);
				BufferPool bf = Database.getBufferPool();
				HeapPage heapPage = (HeapPage) bf.getPage(this.tid, pid, Permissions.READ_WRITE);

				Iterator<Tuple> temp =  heapPage.iterator();

				// Page "pageIndex" contains a tuple; return true
				if(temp.hasNext()){
					return true;
				}
			}

			//No pages contain tuples - return false
			return false;	
		}

		@Override
		public void close() {
			this.i = null;

		}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			//System.out.println("HeapFileIterator open()");

			PageId pid = new HeapPageId(this.fileID, 0);

			BufferPool bf = Database.getBufferPool();

			Page my_page = bf.getPage(this.tid, pid, Permissions.READ_WRITE);
			//System.out.println("Page: " + my_page);

			HeapPage heapPage = (HeapPage) my_page;

			//System.out.println("open3");
			//System.out.println("HeapPage: " + heapPage);

			this.i = heapPage.iterator();
			//System.out.println("open-- start1");
			//System.out.println("open-- end");
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			close();
			open();

		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException,
		NoSuchElementException {
			//System.out.println("HeapFileIterator next()");

			// If no call to open
			if (this.i == null) {
				//System.out.println("current internal iterator is null");
				throw new NoSuchElementException();
			}

			// If the current page has an available tuple
			if (this.i.hasNext()) {
				return this.i.next();
			}

			// If the current page does not have a tuple, move onto the next page that does
			while (this.i.hasNext() == false) {

				if (this.hasNext()){
					//System.out.println("CURRENT PAGE: " + this.curPage);

					this.curPage += 1;
					PageId pid = new HeapPageId(this.fileID, this.curPage);
					BufferPool bf = Database.getBufferPool();
					HeapPage heapPage = (HeapPage) bf.getPage(this.tid, pid, Permissions.READ_WRITE);
					this.i = heapPage.iterator();
				} else{
					System.out.println("external iterator has no next page");
					System.out.println("current page: " + this.curPage);
					throw new NoSuchElementException();
				}	
			}

			return this.i.next(); // is this skipping the first tuple of every page? we'll see!
		}

	}

	/**
	 * Constructs a heap file backed by the specified file.
	 * 
	 * @param f
	 *            the file that stores the on-disk backing store for this heap
	 *            file.
	 */

	public File file;
	public TupleDesc schema;
	public Map<TransactionId, DbFileIterator> iterators;

	public HeapFile(File f, TupleDesc td) {
		this.file = f;
		this.schema = td;
		this.iterators = new HashMap<TransactionId, DbFileIterator>();
	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 * 
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		return this.file;
	}

	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note:
	 * you will need to generate this tableid somewhere ensure that each
	 * HeapFile has a "unique id," and that you always return the same value for
	 * a particular HeapFile. We suggest hashing the absolute file name of the
	 * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 * 
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {
		return this.file.getAbsoluteFile().hashCode();
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		return this.schema;
	}

	/**
	 * Read the specified page from disk.
	 *
	 * @throws IllegalArgumentException if the page does not exist in this file.
	 */
	// see DbFile.java for javadocs
	public Page readPage(PageId pid) {
		byte[] buffer = new byte[BufferPool.getPageSize()];
		FileInputStream fs = null;
		HeapPage page = null;
		HeapPageId hpid = (HeapPageId)pid;

		try{
			fs = new FileInputStream(this.file);
			fs.skip(pid.pageNumber()*BufferPool.getPageSize());
			fs.read(buffer);
			fs.close();
			page = new HeapPage(hpid, buffer);

		} catch (IOException e){
			e.printStackTrace();
		}
		return page;
	}

	/**
	 * Push the specified page to disk.
	 *
	 * @param p The page to write.  page.getId().pageno() specifies the offset into the file where the page should be written.
	 * @throws IOException if the write fails
	 *
	 */
	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		// some code goes here
		// not necessary for lab 1

		byte[] buffer = page.getPageData();
		int off = page.getId().pageNumber()*BufferPool.getPageSize();
		RandomAccessFile file_random = new RandomAccessFile(file, "rw");
		
		file_random.seek(off);
		file_random.write(buffer, 0, BufferPool.getPageSize());
		file_random.close();
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		//System.out.println("HeapFile numPages()");
		//long file_length = this.file.length();
		//System.out.println("  file_length = " + file_length);
		//System.out.println("  bufferPool page size = " + BufferPool.getPageSize());
		return (int) (this.file.length() / BufferPool.getPageSize());
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1

		ArrayList<Page> output = new ArrayList<Page>();
		boolean found_space = false;

		for(int i = 0; i< numPages() ;i++)
		{
			//HeapPageId hpid = new HeapPageId(t.getRecordId().getPageId().getTableId(), i);
			HeapPageId hpid = new HeapPageId(getId(), i);
			Page right_page = Database.getBufferPool().getPage(tid, hpid,Permissions.READ_WRITE);
			HeapPage right_hpage = (HeapPage) right_page;

			if(right_hpage.getNumEmptySlots()>0)
			{
				//RecordId t_rid = new RecordId(hpid, t.getRecordId().tupleno());
				//t.setRecordId(t_rid);
				right_hpage.insertTuple(t);
				//right_hpage.markDirty(true, tid);
				output.add(right_hpage);

				found_space = true;
				break;
			}

		}

		if(!found_space)
		{
			byte[] empty = HeapPage.createEmptyPageData();

			//int tableid = t.getRecordId().getPageId().getTableId();
			int tableid = getId();

			//System.out.println("HeapFile::HeapFile() - tableID: "+tableid);
			HeapPageId hid = new HeapPageId(tableid,numPages());
			HeapPage empty_hpage = new HeapPage(hid, empty);
			empty_hpage.insertTuple(t);

			writePage(empty_hpage);

			//RecordId t_rid = new RecordId(hid, t.getRecordId().tupleno());
			//t.setRecordId(t_rid);

			
			//empty_hpage.markDirty(true, tid);
			output.add(empty_hpage);
		}

		return output;

	}

	// see DbFile.java for javadocs
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
	TransactionAbortedException {
		// some code goes here
		// not necessary for lab1

		ArrayList<Page> output = new ArrayList<Page>();

		RecordId t_rid = t.getRecordId();
		PageId pid = t_rid.getPageId();

		Page right_page = Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
		HeapPage right_hpage = (HeapPage) right_page;

		right_hpage.deleteTuple(t);
		right_hpage.markDirty(true, tid);

		output.add(right_page);

		return output;
	}

	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		//int pageSize, int numPages, TransactionId tid, int fileID
		try {
			HeapFileIterator it = new HeapFileIterator(BufferPool.getPageSize(), this.numPages(), tid, this.getId());
			return it;
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.print("error DBException in creating DBFileIteartor");
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.print("error Transaction Aborted in creating DBFileIterator");
		}

		System.out.println("there was an error,, returning null");
		return null;
	}
}