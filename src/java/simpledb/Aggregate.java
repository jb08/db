package simpledb;

import java.util.*;

import simpledb.Aggregator.Op;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

	DbIterator child;
	int afield;
	int gfield;
	Aggregator.Op aop;
	TupleDesc child_td;

	Aggregator agg;
	DbIterator aggIter;
	boolean grouping;
	
	TupleDesc td;

	private static final long serialVersionUID = 1L;

	/**
	 * Constructor.
	 * 
	 * Implementation hint: depending on the type of afield, you will want to
	 * construct an {@link IntAggregator} or {@link StringAggregator} to help
	 * you with your implementation of readNext().
	 * 
	 * 
	 * @param child
	 *            The DbIterator that is feeding us tuples.
	 * @param afield
	 *            The column over which we are computing an aggregate.
	 * @param gfield
	 *            The column over which we are grouping the result, or -1 if
	 *            there is no grouping
	 * @param aop
	 *            The aggregation operator to use
	 */
	public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
		// some code goes here

		this.child = child;
		this.afield = afield;
		this.gfield = gfield;
		this.aop = aop;

		child_td = child.getTupleDesc();

		Type aggFieldType = child_td.getFieldType(afield);
		
		grouping = (gfield>Aggregator.NO_GROUPING);
		
		Type gbFieldType = null;
		
		if(grouping)
			gbFieldType = child_td.getFieldType(gfield);

			if(aggFieldType == Type.INT_TYPE)
		{
			agg = new IntegerAggregator(gfield, gbFieldType, afield, aop);
		}
		else if(aggFieldType == Type.STRING_TYPE)
		{
			agg = new StringAggregator(gfield, gbFieldType, afield, aop);
		}
		else
		{
			System.out.println("Aggregate::Agregate() failed");
		}
		
		//set up new TupleDesc
		if(grouping)
		{
			Type aggType = child_td.getFieldType(afield);
			String aggName = aop.toString()+child_td.getFieldName(afield);
			
			
			Type[] typeAr = {gbFieldType, aggType};
			String[] fieldAr = {child_td.getFieldName(gfield),aggName};
			
			td = new TupleDesc(typeAr, fieldAr);
		}
		else //not grouping
		{
			Type aggType = child_td.getFieldType(afield);
			String aggName = aop.toString()+child_td.getFieldName(afield);
			
			
			Type[] typeAr = {aggType};
			String[] fieldAr = {aggName};
			
			td = new TupleDesc(typeAr, fieldAr);
			//System.out.println("no_grouping");
		}
		
		
	}

	/**
	 * @return If this aggregate is accompanied by a groupby, return the groupby
	 *         field index in the <b>INPUT</b> tuples. If not, return
	 *         {@link simpledb.Aggregator#NO_GROUPING}
	 * */
	public int groupField() {
		// some code goes here
		
		if(grouping)
			return gfield;
		else
			return Aggregator.NO_GROUPING;
	}

	/**
	 * @return If this aggregate is accompanied by a group by, return the name
	 *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
	 *         null;
	 * */
	public String groupFieldName() {
		// some code goes here
		
		//implement getting group field name in OUTPUT
		if(grouping)
			return td.getFieldName(gfield);
		else
			return null;
	}

	/**
	 * @return the aggregate field
	 * */
	public int aggregateField() {
		// some code goes here
		return afield;
	}

	/**
	 * @return return the name of the aggregate field in the <b>OUTPUT</b>
	 *         tuples
	 * */
	public String aggregateFieldName() {
		// some code goes here
		
		//implement getting aggregate field name in OUTPUT
		
		return td.getFieldName(afield);
	}

	/**
	 * @return return the aggregate operator
	 * */
	public Aggregator.Op aggregateOp() {
		// some code goes here
		return aop;
	}

	public static String nameOfAggregatorOp(Aggregator.Op aop) {
		return aop.toString();
	}

	public void open() throws NoSuchElementException, DbException,
	TransactionAbortedException {
		// some code goes here

		child.open();
		
		while(child.hasNext())
		{
			Tuple nextTuple = child.next();
			agg.mergeTupleIntoGroup(nextTuple);
		}
		super.open();
		aggIter = agg.iterator();
		aggIter.open();
	}

	/**
	 * Returns the next tuple. If there is a group by field, then the first
	 * field is the field by which we are grouping, and the second field is the
	 * result of computing the aggregate, If there is no group by field, then
	 * the result tuple should contain one field representing the result of the
	 * aggregate. Should return null if there are no more tuples.
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		
		Tuple output = null;
		
		if(aggIter.hasNext())
		{
			output = aggIter.next();
		}
		
		return output;
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here

		//child.rewind();
		aggIter.rewind();
	}

	/**
	 * Returns the TupleDesc of this Aggregate. If there is no group by field,
	 * this will have one field - the aggregate column. If there is a group by
	 * field, the first field will be the group by field, and the second will be
	 * the aggregate value column.
	 * 
	 * The name of an aggregate column should be informative. For example:
	 * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
	 * given in the constructor, and child_td is the TupleDesc of the child
	 * iterator.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		return td;
	}

	public void close() {
		// some code goes here
		
		super.close();
		child.close();
		aggIter.close();
		
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
