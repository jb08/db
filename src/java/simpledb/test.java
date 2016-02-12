package simpledb;
import java.io.*;
import java.util.BitSet;

public class test {

	public static void main(String[] argv) {
		//test_1();
		//grpAgg_test();
		//byte_stuff();


	}
	
	public static void byte_stuff()
	{
		byte[] bytes = new byte[1];
		Byte value = Byte.parseByte("00000010",2);
		bytes[0] = value;
		System.out.println(value.toString());
		
		int i = 0;
		
		int which_byte = i/8;
    	int position = i%8;
    	
    	byte get_byte = bytes[which_byte];
    	int get_bit = (get_byte >> position) & 1;
    	//System.out.println(get_bit);
    	
    	//get_byte = get_byte & ~((byte) 0 << position);
    	//bytes[which_byte] = (byte) (bytes[which_byte] &  ~( (byte) 1 << position));
    	bytes[which_byte] = (byte) (bytes[which_byte] |  ( (byte) 1 << position));
    	
    	Byte b = (Byte) bytes[which_byte];
    	
    	System.out.println(b.toString());
		
	}

	public static void grpAgg_test()
	{
		// construct a 3-column table schema
		Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
		String names[] = new String[]{ "s_id", "s_zip", "s_num_courses" };
		TupleDesc descriptor = new TupleDesc(types, names);

		// create the table, associate it with some_data_file.dat
		// and tell the catalog about the schema of this table.
		HeapFile table1 = new HeapFile(new File("students.dat"), descriptor);

		Database.getCatalog().addTable(table1, "students");

		TransactionId tid = new TransactionId();

		SeqScan f = new SeqScan(tid, table1.getId());
		System.out.println("Input file:");

		try {
			// and run it
			f.open();
			while (f.hasNext()) 
			{
				Tuple tup = f.next();
				System.out.print("  tuple-was: "+ tup.toString());
			}
			f.close();
			Database.getBufferPool().transactionComplete(tid);
		} 
		catch (Exception e) 
		{
			System.out.println ("Exception : " + e);
		}

		//try String Aggregator
		StringAggregator agg = new StringAggregator(1, Type.INT_TYPE, 2, Aggregator.Op.COUNT);
		
		try{
			f.open();

			while (f.hasNext()) 
			{
				agg.mergeTupleIntoGroup(f.next());
			}

			
		}
		catch (Exception e)
		{
			System.out.println ("Exception : " + e);
		}
		
		DbIterator it = agg.iterator();
		
		System.out.println("Group by Zip:");
		
		try 
		{
			// and run it
			it.open();
			while (it.hasNext()) 
			{
				Tuple tup = it.next();
				System.out.print("  tuple-was: "+ tup.toString());
			}
			it.close();
		} 
		catch (Exception e) 
		{
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