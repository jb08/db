package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
	
    private TDItem[] schema;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
        
        public boolean equals(Object o){
        	
        	if(o instanceof TDItem)
        	{
        		TDItem item = (TDItem) o;
        		boolean field_types_eq = item.fieldType.equals(this.fieldType);
        		/*boolean field_names_eq = false;
        		
        		if(item.fieldName != null && fieldName !=null)
        			field_names_eq = item.fieldName.equals(this.fieldName);
        		else if(item.fieldName == null && fieldName ==null)
        			field_names_eq = true;
        		
        		if (field_names_eq && field_types_eq) return true;*/
        		
        		return field_types_eq;
        	}
        	
        	return false;
        }
        
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        
    	List<TDItem> td_list = Arrays.asList(schema);
    	
        return td_list.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	
    	//System.out.println("TupleDesc()");
    	
    	this.schema = new TDItem[typeAr.length];
    	
    	for(int i=0; i< schema.length; i++)
    	{
    		//System.out.println("i is: "+i);
    		TDItem to_add = new TDItem(typeAr[i], fieldAr[i]);
    		schema[i] = to_add;
    		//System.out.println(schema[i]);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	
    	this.schema = new TDItem[typeAr.length];
    	
    	for(int i = 0; i < schema.length; i++)
    	{
    		TDItem to_add = new TDItem(typeAr[i], null);
    		schema[i] = to_add;
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        
    	int output = schema.length;
    	
        return output;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	
    	check_range(i); //throws error if out of range
    	
    	TDItem my_TDItem = schema[i];
    	String field_name = my_TDItem.fieldName;
    	
        return field_name;
    }

    private int check_range(int i) throws NoSuchElementException {
    	
    	if(i>=schema.length || i < 0)
    	{
    		System.out.println("Error: index: " + i + "-- out of range in schema.length: " + schema.length);
    		throw new NoSuchElementException();
    	}
    	
    	return 0;
    }
    
    
    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	
    	check_range(i);
    	
    	TDItem my_TDItem = schema[i];
    	Type my_type = my_TDItem.fieldType;
    	
    	//System.out.println(my_type.toString());
    	
        return my_type;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	
    	for(int i = 0; i < schema.length; i++)
    	{
    		TDItem my_TDItem = schema[i];
    		String field_name = my_TDItem.fieldName;
    		
    		if(field_name == null)
    			break;
    		
    		if(field_name.equals(name))
    			return i;
    	}
    	
    	throw new NoSuchElementException();	
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	
    	int size = 0;
    	
    	for(int i = 0; i< schema.length; i++)
    	{
    		Type my_type = schema[i].fieldType;
    		int obj_size = my_type.getLen();
    		
    		size += obj_size;
    	}
    	
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        
    	int new_len = td1.schema.length + td2.schema.length;
    	
    	Type[] typeAr = new Type[new_len];
    	String[] fieldAr = new String[new_len];
    	
    	for(int i=0; i< td1.schema.length; i++)
    	{
    		typeAr[i] = td1.schema[i].fieldType;
    		fieldAr[i] = td1.schema[i].fieldName;
    	}
    	
    	for(int i = 0; i< td2.schema.length; i++)
    	{
    		typeAr[i+ td1.schema.length] = td2.schema[i].fieldType;
    		fieldAr[i+ td1.schema.length] = td2.schema[i].fieldName; 
    	}
    	
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	
    	if(o instanceof TupleDesc)
    	{
    		TupleDesc other = (TupleDesc) o;
    		
    		if(schema.length != other.schema.length)
    			return false;
    		
    		for(int i = 0; i < this.schema.length; i++)
    		{
    			if(! schema[i].equals(other.schema[i]))
    				return false;
    		}
    		
    		return true;
    		
    	}
    	
    	else return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	
    	String output = "";
    	
    	for(int i= 0; i<schema.length; i++)
    	{
    		TDItem my_item = schema[i];
    		output += my_item.toString();
    		
    		if(i+1 !=schema.length)
    			output += ", ";
    	}
    	
        return output;
    }
}
