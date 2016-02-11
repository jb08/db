package simpledb;

import java.io.Serializable;

import simpledb.Predicate.Op;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

	int jp_field1;
	int jp_field2;
	Predicate.Op jp_op;
	
    private static final long serialVersionUID = 1L;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // some code goes here
    	
    	jp_field1 = field1;
    	jp_field2 = field2;
    	jp_op = op;
    	
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
    	
    	Field f1 = t1.getField(jp_field1);
    	Field f2 = t2.getField(jp_field2); 
    	
        boolean output = f1.compare(jp_op, f2);
        
        return output;
    }
    
    public int getField1()
    {
        // some code goes here
        return jp_field1;
    }
    
    public int getField2()
    {
        // some code goes here
        return jp_field2;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return jp_op;
    }
}
