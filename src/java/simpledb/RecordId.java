package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private PageId r_pid;
    private int tuple_num;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
    	
    	r_pid = pid;
    	tuple_num = tupleno;
    	
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        // some code goes here
    	
        return tuple_num;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return r_pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
    	
    	if(o instanceof RecordId)
    	{
    		RecordId r_o = (RecordId) o; 
    		boolean pids_equal = r_pid.equals(r_o.r_pid);
    		
    		boolean tupnos_equal = tuple_num == r_o.tuple_num;
    		
    		//System.out.println("pid: " + pids_equal + "; tupnos:" + tupnos_equal);
    		
    		return pids_equal && tupnos_equal;
    	}
    	else
    	{
    		return false;
    	}
    	
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
    	
    	String s_page_num = Integer.toString(r_pid.pageNumber());
    	String s_tuple_num = Integer.toString(tuple_num);
    	String output = s_page_num + s_tuple_num;
    	
    	return output.hashCode();
    	
        //throw new UnsupportedOperationException("implement this");

    }

}
