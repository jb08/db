package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

	TransactionId tid;
	DbIterator child;
	TupleDesc FNresult;
	boolean hasNotBeenUsed;
	
    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	
    	this.tid = t;
    	this.child = child;
    	this.hasNotBeenUsed = true;
    	
    	Type[] typeAr = new Type[]{Type.INT_TYPE};
		String[] fieldAr = new String[]{"Num_deleted_tuples"};
		FNresult = new TupleDesc(typeAr, fieldAr);
    	
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return FNresult;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	
    	child.open();
    	super.open();
    }

    public void close() {
        // some code goes here
    	
    	child.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	
    	child.rewind();
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
		
		int count = 0; 
		
		if(!this.hasNotBeenUsed)
		{
			return null;
		}
		
		try
		{
			while(child.hasNext())
			{
				Database.getBufferPool().deleteTuple(tid, child.next());
				count +=1;
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		//reporting purposes
		Tuple t = new Tuple(FNresult);
		t.setField(0, new IntField(count));
		this.hasNotBeenUsed = false;
		
		return t;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	this.child = children[0];
    }

}
