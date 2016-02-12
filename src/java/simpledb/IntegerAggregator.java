package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;

	int ia_gbfield;
	Type ia_gbfieldtype;
	int ia_afield;
	Op ia_what;

	boolean no_grouping;
	int count_if_not_grouping;

	TupleDesc tup_desc;

	HashMap<Field,Integer> gbAgg;
	HashMap<Field,Integer> numElements;
	HashMap<Field,Integer> sum;

	/**
	 * Aggregate constructor
	 * 
	 * @param gbfield
	 *            the 0-based index of the group-by field in the tuple, or
	 *            NO_GROUPING if there is no grouping
	 * @param gbfieldtype
	 *            the type of the group by field (e.g., Type.INT_TYPE), or null
	 *            if there is no grouping
	 * @param afield
	 *            the 0-based index of the aggregate field in the tuple
	 * @param what
	 *            the aggregation operator
	 */

	public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here

		ia_gbfield = gbfield;
		ia_gbfieldtype = gbfieldtype;
		ia_afield = afield;
		ia_what = what;

		gbAgg = new HashMap<Field, Integer>();
		numElements = new HashMap<Field, Integer>();
		sum = new HashMap<Field, Integer>();
		no_grouping = (gbfieldtype == null || gbfield == NO_GROUPING);
		count_if_not_grouping = 0;

		tup_desc = null;

	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 * 
	 * @param tup
	 *            the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		// some code goes here

		//save tupledescriptor for creating final table
		if(tup_desc == null)
		{
			tup_desc = tup.getTupleDesc();
		}

		//not grouping - just increment count of rows
		Field f;

		if(no_grouping)
		{
			f = new IntField(Aggregator.NO_GROUPING);
		}
		else
		{
			f = tup.getField(ia_gbfield);
		}

		//grouping - add or increment Field in Hashmap


		Field tf = tup.getField(ia_afield);
		IntField itf = (IntField) tf;
		int tupVal = itf.getValue();
		int val = 0;
		int count = 0;
		int total = 0;
		boolean first_key = true; 

		if(gbAgg.containsKey(f))
		{
			val = gbAgg.get(f);
			first_key = false;
		}	

		if (ia_what==Op.MIN)
		{
			if(first_key) val = Integer.MAX_VALUE;
			val = Math.min(val, tupVal);
		}
		else if (ia_what==Op.MAX)
		{
			if(first_key) val = Integer.MIN_VALUE;
			val = Math.max(val, tupVal);
		}
		else if (ia_what==Op.SUM)
		{
			val += tupVal;
		}
		else if (ia_what==Op.AVG)
		{
			if(numElements.containsKey(f))
			{
				count = numElements.get(f);
				total = sum.get(f);
			}

			total += tupVal;
			val = total / (count+1);
			numElements.put(f,(count+1));
			sum.put(f, total);

		}
		else if (ia_what==Op.COUNT)
		{
			val+=1;
		}

		gbAgg.put(f,val);
	}

	/**
	 * Create a DbIterator over group aggregate results.
	 * 
	 * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
	 *         if using group, or a single (aggregateVal) if no grouping. The
	 *         aggregateVal is determined by the type of aggregate specified in
	 *         the constructor.
	 */
	public DbIterator iterator() {
		// some code goes here

		TupleDesc table_td;

		//Create TupleDesc for final table
		if(ia_gbfield>Aggregator.NO_GROUPING)
		{
			Type[] typeAr = new Type[2];
			typeAr[0] = ia_gbfieldtype;
			typeAr[1] = Type.INT_TYPE;

			String[] fieldAr = new String[2];
			String f_name = tup_desc.getFieldName(ia_gbfield);
			fieldAr[0] = f_name;
			fieldAr[1] = tup_desc.getFieldName(ia_afield);

			table_td = new TupleDesc(typeAr, fieldAr);
		}
		else
		{
			Type[] typeAr = new Type[1];
			typeAr[0] = Type.INT_TYPE;

			String[] fieldAr = new String[1];
			fieldAr[0] = tup_desc.getFieldName(ia_afield);

			table_td = new TupleDesc(typeAr, fieldAr);
		}



		//Set up container that can be iterated
		ArrayList<Tuple> arr = new ArrayList<Tuple>();

		Set<Field> fields = gbAgg.keySet();

		for(Field field : fields)
		{
			int value = gbAgg.get(field);
			Tuple row = new Tuple(table_td);

			if(ia_gbfield>Aggregator.NO_GROUPING)
			{
				//first column: group by field
				row.setField(0, field);
				//second column: count
				row.setField(1, new IntField(value));
			}
			else
			{
				row.setField(0, new IntField(value));
			}
			
			
			arr.add(row);
		}

		//Other case: no_grouping
		//Not Implemented

		TupleIterator it = new TupleIterator(table_td, (Iterable<Tuple>) arr);

		return it;
	}

}