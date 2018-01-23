import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Speed {
	
	public static class MapClass extends Mapper<IntWritable,Text,Text,Text>
	   {
	      public void map(IntWritable key, Text value, Context context)
	      {	    	  
	         try{ 
	            String[] str = value.toString().split(",");	 
	            //int key = Integer.parseInt(str[0]);
	            String s1=str[1];
	            context.write(new Text(str[0]), new Text(s1));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,IntWritable,Text,Text>
	   {
		    private int offencepercent =0;
		    
		    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		      int offencecount=0;
		      int total=0;
		      for(IntWritable val:values)
		      {
		    	 if(val.get()>65)
		    	 {
		    		 offencecount++;
		    	 }
		    	 total++;
		      }
		offencepercent=offencecount*100/total;
		String percent_value=String.format("%",offencepercent);
		String valwithsign=percent_value +"%";
		context.write(key,new Text(valwithsign));
		   
		         /*for (LongWritable val : values)
		         {       	
		        	sum += val.get();      
		         }
		         
		      result.set(sum);		      
		      context.write(key, result);
		      //context.write(key, new LongWritable(sum));*/
		      
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "Speed");
		    job.setJarByClass(Speed.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setNumReduceTasks(2);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}