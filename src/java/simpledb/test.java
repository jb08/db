package simpledb;
import java.io.*;

public class test {
	
    public static void main(String[] argv) {
    	//test_1();
    	grpAgg_test();
    	
    	
    }
    
	public static void grpAgg_test()
	{
		// construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "s_id", "s_age", "s_gpa" };
        TupleDesc descriptor = new TupleDesc(types, names);
        
        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("students.dat"), descriptor);
        
        Database.getCatalog().addTable(table1, "students");
        
        TransactionId tid = new TransactionId();
        
        SeqScan f = new SeqScan(tid, table1.getId());
        
        try {
            // and run it
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println("tuple-was: "+ tup.toString());
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
    }
    
	public static void test_1()
	{
		// construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "s_id", "s_age", "s_gpa" };
        TupleDesc descriptor = new TupleDesc(types, names);
        System.out.println("TD = " + descriptor.toString());
        
        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("students.dat"), descriptor);
        TupleDesc my_descriptor = table1.getTupleDesc();
        //System.out.println("(still) TD = " + my_descriptor.toString());
        System.out.println("HeapFile num_pages = " + table1.numPages());
        System.out.println("---------");
        
        //System.out.println("Before first call to Database");
		//int num = Database.getCatalog().tables.size();
		//System.out.println(num);
        Database.getCatalog().addTable(table1, "students");
        //num = Database.getCatalog().tables.size();
        
        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        //TransactionId tid0 = new TransactionId();
        TransactionId tid = new TransactionId();
        
        //System.out.println("tid is: "+ tid.toString());
        //System.out.println("tid is: "+ tid.getId());
        int id = table1.getId();
        //System.out.println("id is: "+ id);
        SeqScan f = new SeqScan(tid, table1.getId());
        
        try {
            // and run it
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println("tuple-was: "+ tup.toString());
                //throw new Exception("error: exception of my chooosing... :/");
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
    }   
}