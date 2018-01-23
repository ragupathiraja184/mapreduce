

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyPartitioner extends Configured implements Tool
{
public static class Mapclass extends Mapper<LongWritable,Text,Text,Text>
{
public void map(LongWritable key,Text value,Context context) throws InterruptedException,IOException
{
		//Text stock_sym = new Text();
		//FloatWritable percent = new FloatWritable();
try
{
	String[] str=value.toString().split(",");
	String gender=str[3];
	context.write(new Text(gender), new Text(value));
}
catch(Exception e)
{
	System.out.println(e.getMessage());
}
}
}
public static class Reduceclass extends Reducer<Text,Text,Text,IntWritable>
{
	public int max=0;
	private Text outputkey=new Text();
	public void Reduce(Text key,Iterable<Text>values,Context context) throws InterruptedException, IOException
	{
		for(Text val: values)
		{
			String []str=val.toString().split(",");
			if(Integer.parseInt(str[4])>max)
			{
				max=Integer.parseInt(str[4]);
				String mkey=str[3]+","+str[1]+","+str[2];
				outputkey.set(mkey);
				
			}
		}
		context.write(outputkey,new IntWritable(max));
		}
public static class CarderPartitioner extends
Partitioner < Text, Text >
{
   public int getPartition(Text key, Text value, int numReduceTasks)
   {
      String[] str = value.toString().split(",");
      int age = Integer.parseInt(str[2]);


      if(age<=20)
      {
         return 0 % numReduceTasks;
      }
      else if(age>20 && age<=30)
      {
         return 1 % numReduceTasks ;
      }
      else
      {
         return 2 % numReduceTasks;
      }
   }
}

public int run(String[] arg) throws Exception {
	Configuration conf = new Configuration();
    //conf.set("name", "value")
    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
    Job job = Job.getInstance(conf, "partition");
    job.setJarByClass(MyPartitioner.class);
    job.setMapperClass(Mapclass.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    //job.setNumReduceTasks(3);
    //job.setCombinerClass(ReduceClass.class);
    job.setReducerClass(Reduceclass.class);
    job.setPartitionerClass(CarderPartitioner.class);
    job.setNumReduceTasks(3);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    int res = ToolRunner.run(new Configuration(), new MyPartitioner(),arg);
    System.exit(0);
	return 0;
}
  
}
public int run(String[] arg0) throws Exception {
	// TODO Auto-generated method stub
	return 0;
}
}
