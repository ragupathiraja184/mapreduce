import java.io.*;
//import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Transaction {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{ 
	            String[] str = value.toString().split(";");	
	            String s1=str[0]+","+str[1]+","+str[8];
	            String s="k"; 
	            context.write(new Text(s), new Text(s1));
	            
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,Text,Text,Text>
	   {
		    
		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		      //int offencecount=0;
		      //int total=0;
			  //private int cost =0;
			    String s="";
			    //long amt=Long.parseLong(ss[2]);
			    long max_amt=0;
			    //long amt=0;
			    //String custid=ss[1];
			    //DateFormat df=new DateFormat("yy-mm-dd hh:mm:ss");
			    //Date date=df.parseDate(ss[0]);
			    //String date=ss[0];
			    //String cust_id=ss[1];
		      for(Text val:values)
		      {
		    	  String []ss=val.toString().split(",");
		    	  long amt=Long.parseLong(ss[2]);
		    	 if(amt>max_amt)
		    	 {
		    	  max_amt=amt;
		  
			     s=ss[0]+","+ss[1]+","+max_amt;  
		    	 
		      }
		      }
		      
		/*offencepercent=offencecount*100/total;
		String percent_value=String.format("%",offencepercent);
		String valwithsign=percent_value +"%";*/
		//context.write(key,new LongWritable(max_amt));
		   
		         /*for (LongWritable val : values)
		         {       	
		        	sum += val.get();      
		         }
		         
		      result.set(sum)*/;		      
		      context.write(key, new Text(s));
		      //context.write(key, new LoLongWritablengWritable(sum));
		      
		    
	   }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "Transaction");
		    job.setJarByClass(Transaction.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setNumReduceTasks(2);
		    job.setOutputKeyClass(Text.class);
		  
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}