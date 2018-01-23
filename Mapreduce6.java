
import java.io.*;

import java.util.TreeMap;
//import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Mapreduce6 {
	public static class countMapper extends
	Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
			try {
			String[] str = value.toString().split("\t");
			//String year= str[7];
			//String key1="k";
			String case_status=str[1];
			//String r1=year+","+case_status;
		context.write(new Text(case_status),new Text(value));
	           
         }
         catch(Exception e)
         {
            System.out.println(e.getMessage());
         }
      }
   }
	public static class CaderPartitioner extends Partitioner < Text, Text >
	   {
	      @Override
	      public int getPartition(Text key, Text value, int NumReduceTasks)
	      {
	         String[] str = value.toString().split("\t");
	         int year = Integer.parseInt(str[7]);


	         if(year==2011)
	         {
	            return 0 % NumReduceTasks;
	         }
	         else if(year==2012)
	         {
	            return 1 % NumReduceTasks ;
	         }
	         else if(year==2013)
	         {
	            return 2 % NumReduceTasks;
	         }
	         else if(year==2014)
	         {
	        	 return 3 % NumReduceTasks;
	         }
	         else if(year==2015)
	         {
	        	 return 4 % NumReduceTasks;
	         }
	         else
	         {
			return 5%NumReduceTasks;
	         
	         }
	      }
	   }
	public static class countReducer extends
	Reducer<Text, Text, NullWritable, Text> {
		public TreeMap<Long,Text> tm=new TreeMap<Long,Text>();
		Double certified_percent=0.0;
		Double denied_percent=0.0;
		Double certified_withdrawnpercent=0.0;
		 Double withdrawn_percent=0.0;
		 long allcount=0;
			long certified=0;
			long certified_withdrawn=0;
			//String case_status="";
			long withdrawn=0;
			long denied=0;
			Double certified_percent1=0.0;
			Double denied_percent1=0.0;
			Double certified_withdrawnpercent1=0.0;
			 Double withdrawn_percent1=0.0;
			 long allcount1=0;
				long certified1=0;
				long certified_withdrawn1=0;
				//String case_status="";
				long withdrawn1=0;
				long denied1=0;

			String result1="";
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			
			/*String s2="";
			String s3="";
			String s4="";*/

			
			String result="";
			long year1=0;
			String case_status="";
			for(Text val:values)
			{
				String[] s21=val.toString().split("\t");
			//StringTokenizer sp1=new StringTokenizer(s2,",");
				year1=Long.parseLong(s21[7]);
			 case_status=s21[1];
			allcount++;
				//while(sp1.hasMoreTokens()){
			if(s21[1].equals("CERTIFIED WITHDRAWN"))
			{
			certified_withdrawn+=1;
			}
			if(s21[1].equals("CERTIFIED"))
			{
				certified++;
			}
			if(s21[1].equals("WITHDRAWN"))
			{
				withdrawn++;
			}
			if(s21[1].equals("DENIED"))
			{
				denied++;
			}
			}
		
			//	certified_percent=(double) ((certified/allcount)*100);
			//denied_percent=(double) ((denied/allcount)*100);
			//certified_withdrawnpercent=(double) ((certified_withdrawn/allcount)*100);
			//withdrawn_percent=(double) ((withdrawn/allcount)*100);
			//s1=certified_percent.toString();
			//s2=denied_percent.toString();
			//s3=certified_withdrawnpercent.toString();
			//s4=withdrawn_percent.toString();
			result=case_status+","+String.format("%d,%d,%d,%d,%d", allcount,certified,denied,certified_withdrawn,withdrawn);
			tm.put(new Long(year1),new Text(result));
			//ontext.write(NullWritable.get(), new Text(result));
		}
	
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		for(Long t:tm.keySet())
		{   String[] srt=tm.get(t).toString().split(",");
		    allcount1=Long.parseLong(srt[1]);
		    certified1=Long.parseLong(srt[2]);
		    denied1=Long.parseLong(srt[3]);
		    certified_withdrawn1=Long.parseLong(srt[4]);
		    withdrawn1=Long.parseLong(srt[5]);
			certified_percent1=(double) ((certified1/allcount1)*100);
			denied_percent1=(double) ((denied1/allcount1)*100);
			certified_withdrawnpercent1=(double) ((certified_withdrawn1/allcount1)*100);
			withdrawn_percent1=(double) ((withdrawn1/allcount1)*100);
			result1=srt[0]+"\t"+String.format("%d,%f,%f,%f,%f", allcount1,certified1,denied1,certified_withdrawn1,withdrawn1);
			context.write(NullWritable.get(), new Text(result1));
		}
	}
	} 
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    //conf.set("name", "value")
    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
    Job job = Job.getInstance(conf, "case_statuspercentage");
    job.setJarByClass(Mapreduce6.class);
    job.setMapperClass(countMapper.class);
    //job.setCombinerClass(countReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setPartitionerClass(CaderPartitioner.class);
    job.setReducerClass(countReducer.class);
    job.setNumReduceTasks(6);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
