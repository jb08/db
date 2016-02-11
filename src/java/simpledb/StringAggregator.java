package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

	int sa_gbfield;
	Type sa_gbfieldtype;
	int sa_afield;
	Op sa_what;

	boolean no_grouping;
	int count_if_not_grouping;

	TupleDesc tup_desc;



	HashMap<Field,Integer> gbAgg;

	private static final long serialVersionUID = 1L;

	/**
	 * Aggregate constructor
	 * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
	 * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
	 * @param afield the 0-based index of the aggregate field in the tuple
	 * @param what aggregation operator to use -- only supports COUNT
	 * @throws IllegalArgumentException if what != COUNT
	 */

	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here

		sa_gbfield = gbfield;
		sa_gbfieldtype = gbfieldtype;
		sa_afield = afield;
		sa_what = what;

		gbAgg = new HashMap<Field, Integer>();
		no_grouping = (gbfieldtype == null || gbfield == NO_GROUPING);
		count_if_not_grouping = 0;
		

		tup_desc = null;

		//throw IllegalArgumentException
		if(what != Aggregator.Op.COUNT)
		{
			throw new IllegalArgumentException("what != COUNT");
		}
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the constructor
	 * @param tup the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		// some code goes here

		//save tupledescriptor for creating final table
		if(tup_desc == null)
		{
			tup_desc = tup.getTupleDesc();
		}

		//not grouping - just increment count of rows
		if(no_grouping)
		{
			count_if_not_grouping +=1;
			return;
		}

		//grouping - add or increment Field in Hashmap
		Field f = tup.getField(sa_gbfield);

		if(gbAgg.containsKey(f))
		{
			gbAgg.put(f, gbAgg.get(f)+1);
		}
		else
		{
			gbAgg.put(f,1);
		}
	}

	/**
	 * Create a DbIterator over group aggregate results.
	 *
	 * @return a DbIterator whose tuples are the pair (groupVal,
	 *   aggregateVal) if using group, or a single (aggregateVal) if no
	 *   grouping. The aggregateVal is determined by the type of
	 *   aggregate specified in the constructor.
	 */
	public DbIterator iterator() {

		//Create TupleDesc for final table
		Type[] typeAr = new Type[2];
		typeAr[0] = sa_gbfieldtype;
		typeAr[1] = Type.INT_TYPE;

		String[] fieldAr = new String[2];
		fieldAr[0] = tup_desc.getFieldName(sa_gbfield);
		fieldAr[1] = tup_desc.getFieldName(sa_afield);

		TupleDesc table_td = new TupleDesc(typeAr, fieldAr);

		//Set up container that can be iterated
		ArrayList<Tuple> arr = new ArrayList<Tuple>();

		Set<Field> fields = gbAgg.keySet();

		for(Field field : fields)
		{
			int value = gbAgg.get(field);
			Tuple row = new Tuple(table_td);

			//first column: group by field
			row.setField(0, field);
			//second column: count
			row.setField(1, new IntField(value));

			arr.add(row);
		}
		
		//Other case: no_grouping
		//Not Implemented

		TupleIterator it = new TupleIterator(table_td, (Iterable<Tuple>) arr);

		return it;
	}

}
