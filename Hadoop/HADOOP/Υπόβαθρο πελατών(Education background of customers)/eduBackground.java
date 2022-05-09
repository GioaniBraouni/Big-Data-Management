
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class eduBackground{
	
	public static void main(String [] args) throws Exception
	{
		Configuration configuration =new Configuration();
		Path inputFolder = new Path(args[0]);
		Path outputFolder = new Path(args[1]);
		
		Job job =new Job(configuration,"wordcount");
		job.setJarByClass(eduBackground.class);
		job.setMapperClass(eduBackgroundMapper.class);
		job.setReducerClass(eduBackgroundReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setSortComparatorClass(EduBackgroundComparator.class);

		FileInputFormat.addInputPath(job, inputFolder);
		FileOutputFormat.setOutputPath(job, outputFolder);
		System.exit(job.waitForCompletion(true)?0:1);
	}

	public static class EduBackgroundComparator extends
			Text.Comparator {
		public int compare (byte [] b1, int s1, int l1, byte [] b2, int s2, int l2) {
			return super.compare (b1, s1, l1, b2, s2, l2);
		}
	}

	public static class eduBackgroundMapper extends Mapper<Object, Text, Text, IntWritable>{
		public int curr_line = 0;
		public void map(Object key, Text value, Context con) throws IOException, InterruptedException
		{
			if (curr_line != 0) 
			{
				con.write(new Text(value.toString().split(";")[2]), new IntWritable(1));
			}
			curr_line++;
		}
	}
	
	public static class eduBackgroundReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
		{
			int sum = 0;
			for(IntWritable value : values)
			{
				sum += value.get();
			}
			con.write(word, new IntWritable(sum));
		}
	}
}
