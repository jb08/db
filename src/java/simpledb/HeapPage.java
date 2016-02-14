package simpledb;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte header[];
    final Tuple tuples[];
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock=new Byte((byte)0);
    
    boolean dirtyPage;
    TransactionId dirty_causing_tid;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */    
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        //System.out.println("HeapPage() -- open");
    	this.pid = id;
    	//System.out.println("..step1");
    	//System.out.println("HeapPage::HeapPage() - tableID: "+id.getTableId());
    	this.td = Database.getCatalog().getTupleDesc(id.getTableId());
    	//System.out.println("..step2");
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        
        /*System.out.println("start ---");*/
        for (int i=0; i<header.length; i++)
        {
        	
        	header[i] = dis.readByte();
        }
        
        //System.out.println("header.length: " + header.length);
        //System.out.println("getHeaderSize: " + this.getHeaderSize());
        
        /*for(int i=0; i< this.getHeaderSize(); i++)
        {
        	if(this.isSlotUsed(i))
        	{
        		System.out.println(i);
        	}
        }*/
        //System.out.println("end ---");
        
        tuples = new Tuple[numSlots]; //good
        //tuples = new Tuple[numSlots+1]; //error  
         
       dirtyPage = false;
       dirty_causing_tid = null;
        
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();
        //System.out.println("..step3");
        setBeforeImage();
        //System.out.println("HeapPage() -- close");
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {        
        // some code goes here
    	double tuple_size_bytes = td.getSize();
    	double page_size_bits = BufferPool.getPageSize()*8;
    	double ans_unrounded = page_size_bits / (tuple_size_bytes * 8 + 1);
    			
    	double ans_rounded = Math.floor(ans_unrounded);
    	//System.out.println("HeapPage getNumTuples(): "+ ans_rounded);
   
        return (int) ans_rounded;
    }
    
    /** standard toString method */
    public String toString()
    {
    	String output = "";
    	output += "HeapPageId: " + pid + "; ";
        output += "TupleDesc: " + td + "; ";
        output += "header[]: " + header + "; ";
        output += "tuples[]: " + tuples + "; ";
        output += "numSlots: " + numSlots + "\n";
    	
    	return output;
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {        
        
    	int result = (int) Math.ceil((double)numSlots / 8); //good
    	//int result = (int) Math.ceil(numSlots / 8); //error
    	
    	//System.out.println("HeapPage getHeaderSize(): " + result);
        return result;
                 
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
    // some code goes here
    	
    return pid;
    	
    //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page;  the tuple should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
    	
    	boolean found_tuple = false;

    	for(int i = 0; i< tuples.length; i++)
    	{
    		if (isSlotUsed(i))
    		{
    			Tuple j = tuples[i];
    			
    			RecordId j_rid = j.getRecordId();
    			RecordId t_rid = t.getRecordId();
    			
    			//found tuple to delete
    			if(j_rid.equals(t_rid))
    			{
    				markSlotUsed(i, false);
    				found_tuple = true;
    				//markDirty(true,null);
    				break;
    			}
    		}
    	}
    	
    	if(!found_tuple) throw new DbException("Unable to find tuple to delete");
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
    	
    	int num_empty = getNumEmptySlots();
    	//System.out.println("HeapPage insertTuple() num_empty:" + num_empty);
    	
    	if(num_empty==0)
    	{
    		throw new DbException("No empty slots on HeapPage");
    	}
    	
    	boolean found_slot = false;

    	for(int i = 0; i< tuples.length; i++)
    	{
    		if (!isSlotUsed(i))
    		{
    			int tup_no_on_page = i;
    			RecordId new_rid = new RecordId(pid,tup_no_on_page);
    			t.setRecordId(new_rid);
    			tuples[i] = t;
    			markSlotUsed(i,true);
    			//markDirty(true,null);
    			//num_empty = getNumEmptySlots();
    			found_slot = true;
    			break;	
    		}
    	}
    	
    	if(!found_slot) throw new DbException("Unable to find empty slot"); 
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
	// not necessary for lab1
    	
    	dirtyPage = dirty;
        dirty_causing_tid = tid;
    	
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
	// Not necessary for lab1
        if(dirtyPage)
        {
        	return dirty_causing_tid;
        }
        else
        {
        	return null;
        }
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
    	
    	//int h_len = header.length;
    	//System.out.println("header length: " + h_len);
    	
    	int num_tuples = tuples.length;
    	//System.out.println("num_tuples: " + num_tuples);
    	
    	//byte one_byte = header[0];
    	//System.out.println(one_byte);
    	//String one_byte_s = Integer.toBinaryString(one_byte);
    	//System.out.println(one_byte_s);
    	//System.out.println(one_byte_s.length());
    	
    	int count = 0;
    	
    	for(int i = 0; i< num_tuples; i++)
    	{
    		if (!isSlotUsed(i))
    		{
    			count = count + 1;
    		}
    	}
        return count;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
    	
    	int which_byte = i/8;
    	int position = i%8;
    	
    	int get_byte = header[which_byte];
    	//System.out.println(get_byte);
    	int get_bit = (get_byte >> position) & 1;
    	
    	if(get_bit == 1)
    	{
    		//System.out.println("yes - slot is used");
    		return true;
    	}
    	else
    	{
    		//System.out.println("no - slot is not used");
    		return false;
    	}
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
		
		int which_byte = i/8;
    	int position = i%8;
    	
    	Byte get_byte = (Byte) header[which_byte];
    	
    	if(value)
    	{
    		//mark bit 1
    		get_byte =  (byte) (get_byte |  ( (byte) 1 << position));
    	}
    	else
    	{
    		//mark bit 0
    		get_byte =  (byte) (get_byte &  ~( (byte) 1 << position));
    	}
    	
    	header[which_byte] = get_byte;
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
    	//System.out.println("HeapPage iterator()");
    	//System.out.println("HeapPage iterator: start");
    	
    	List<Tuple> arrAsList = Arrays.asList(tuples);
    	ArrayList<Tuple> arrList = new ArrayList<Tuple>(arrAsList);
    	//arrAsList.removeAll()
    	
    	//System.out.println("size before removal of nulls: " + arrList.size());
    	
    	arrList.removeAll(Collections.singleton(null));
    	
    	//System.out.println("size after removal of nulls: " + arrList.size());
    	
    	return arrList.iterator();
    	
    }

}

