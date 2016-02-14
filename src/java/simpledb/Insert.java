package simpledb;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

	TransactionId tid;
	DbIterator child;
	int tableid;
	TupleDesc FNresult;
	
	
	
	private static final long serialVersionUID = 1L;

	/**
	 * Constructor.
	 * 
	 * @param t
	 *            The transaction running the insert.
	 * @param child
	 *            The child operator from which to read tuples to be inserted.
	 * @param tableid
	 *            The table in which to insert tuples.
	 * @throws DbException
	 *             if TupleDesc of child differs from table into which we are to
	 *             insert.
	 */
	public Insert(TransactionId t,DbIterator child, int tableid)
			throws DbException {
		// some code goes here
		
		this.tid = t;
		this.child = child;
		this.tableid = tableid;
		
		Type[] typeAr = new Type[]{Type.INT_TYPE};
		String[] fieldAr = new String[]{"Num_inserted_tuples"};
		FNresult = new TupleDesc(typeAr, fieldAr);
	}

	public TupleDesc getTupleDesc() {
		// some code goes here
		
		//return Database.getCatalog().getTupleDesc(tableid);
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
		//super.rewind(); //method doesn't exist
		
	}

	/**
	 * Inserts tuples read from child into the tableid specified by the
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
		
		int count = 0; 
		int abc = 0;
		
		try
		{
			
			abc +=1;
			
			while(child.hasNext())
			{
				Database.getBufferPool().insertTuple(tid, tableid, child.next());
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
		
		return t;
	}

	@Override
	public DbIterator[] getChildren() {
		// some code goes here
		return null;
	}

	@Override
	public void setChildren(DbIterator[] children) {
		// some code goes here
	}
}
