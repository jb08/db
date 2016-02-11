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
	int count;

	TupleDesc tup_desc;

	HashMap<Field,Integer> gbAgg;
    
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
		no_grouping = (gbfieldtype == null || gbfield == NO_GROUPING);
		count_if_not_grouping = 0;
		count = 0;

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
    	count++;
    	
    	
    	//save tupledescriptor for creating final table
		if(tup_desc == null)
		{
			tup_desc = tup.getTupleDesc();
		}

		//not grouping - just increment count of rows
		if(no_grouping)
		{
			//what to do with ops?
		}

		//grouping - add or increment Field in Hashmap
		Field f = tup.getField(ia_gbfield);
		
		Field tf = tup.getField(ia_afield);
		IntField itf = (IntField) tf;
		int tupVal = itf.getValue();
	
		if(gbAgg.containsKey(f))
		{
			int val = gbAgg.get(f);
	
			if (ia_what==Op.MIN)
        		val = Math.min(val, tupVal); 
			else if (ia_what==Op.MAX)
				val = Math.max(val, tupVal);
			else if (ia_what==Op.SUM)
			{
				val += tupVal;
			}
			else if (ia_what==Op.AVG)
			{
				//faulty - count can't work for all groups
				int current_sum = val*(count-1);
				current_sum += tupVal;
				val = current_sum / count;
			}
			/*else if (ia_what==Op.COUNT)
				System.out.println("IntAgg Count");*/
			
			gbAgg.put(f, val);
		}
		else
		{
			gbAgg.put(f,tupVal);
		}
    	
    	
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
        
    	//Create TupleDesc for final table
    	Type[] typeAr = new Type[2];
    	typeAr[0] = ia_gbfieldtype;
    	typeAr[1] = Type.INT_TYPE;

    	String[] fieldAr = new String[2];
    	String f_name = tup_desc.getFieldName(ia_gbfield);
    	fieldAr[0] = f_name;
    	fieldAr[1] = tup_desc.getFieldName(ia_afield);

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