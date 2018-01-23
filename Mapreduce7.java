import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Mapreduce7 {
		public static class MapClass extends Mapper<LongWritable, Text, LongWritable, Text>
		{
			public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
			{
				String[] str = values.toString().split("\t");
			    Long year=Long.parseLong(str[7]);
			    //String job_title=str[4];
				context.write(new LongWritable(year), new Text(values));
			}
		}
		public static class ReduceClass extends Reducer<LongWritable, Text, LongWritable, LongWritable>
		{
		  public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
			{
			  
			long count=0;
			long year=0;
			  for(Text val:values) 
			  {
			   String[] str=val.toString().split("\t");
			   String case_status=str[1];
			   if(case_status.equals("CERTIFIED")||case_status.equals("CERTIFIED_WITHDRAWN")||case_status.equals("DENIED")||case_status.equals("WITHDRAWN"))
			   {
				   count++;
			   }	   
	           year=Long.parseLong(str[7]);
			}
			  context.write(new LongWritable(year),new LongWritable(count));
		}
		  
		}
		public static void main(String[] args) throws Exception
		{
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf,"DATA ENGINEER YEARWISE");
			job.setJarByClass(Mapreduce7.class);
			job.setMapperClass(MapClass.class);
			job.setReducerClass(ReduceClass.class);
			job.setNumReduceTasks(3);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(LongWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}