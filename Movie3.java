

import java.io.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Movie3 {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
		//private Text str1 = new Text();
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(",");
	            String str1 ="my_key";
	            //long vol = Long.parseLong(str[8]);
	            String str2 = str[1]+","+str[2]+","+str[3]+","+str[4];
	            context.write(new Text(str1),new Text(str2));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,Text,Text,LongWritable>
	   {
		    private LongWritable result = new LongWritable();
		    
		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		      long count = 0;
		      float temp_val = 0;
		      //String st= "";
		         for ( Text val : values)
		        	
		         {   
		        	 //String[] str3=val.toString().split(",");
		        	 //FloatWritable value = new FloatWritable(Float.parseFloat(str3[2]));
		        	 
		        	 //temp_val = value.get(); 
		 			//if (temp_val > 3.9) {
		 			//count++;
		        	 
		        	 String[] str3=val.toString().split(",");
		        	 LongWritable value = new LongWritable(Integer.parseInt(str3[3]));
		        	 
		        	 temp_val = value.get(); 
		 			if (temp_val > 5400) {
		 				count += 1;
		 			}     
		 			
		        	 
		         }
		      
		      
		        
		      result.set(count);
		      
		      context.write(key, result);
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "Volume Count");
		    job.setJarByClass(Movie3.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    //job.setNumReduceTasks(0);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}