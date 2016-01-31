package simpledb;

import java.io.Serializable;

import java.util.Arrays;
import java.util.Iterator;

import simpledb.TupleDesc.TDItem;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {
	
    private static final long serialVersionUID = 1L;
    private Field[] record;
    private TupleDesc tup_desc;
    private RecordId rid;
    
    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        tup_desc = td;
        record = new Field[tup_desc.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
   
    	return tup_desc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
    	record[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return record[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // some code goes here
        
    	String output = "";
    	
    	for(int i = 0; i< record.length; i++)
    	{
    		output += record[i].toString();
    		
    		if(i+1 != record.length) output += " ";
    		else output += "\n";
    	}
    	
    	/*for(Field f:record)
    	{
    		output += f.toString();
    		output += " ";
    	}
    	output += "\n";*/
    	
    	return output;
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
    	Iterable<Field> iterable = Arrays.asList(record);
        return iterable.iterator();
    }
    
    /**
     * reset the TupleDesc of the tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        tup_desc = td; //TODO (unclear)
    }
}
