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



public class wineBuyer{
	
	public static void main(String [] args) throws Exception
	{
		Configuration configuration =new Configuration();
		Path inputFolder = new Path(args[0]);
		Path outputFolder = new Path(args[1]);
		
		Job job =new Job(configuration,"wordcount");
		job.setJarByClass(wineBuyer.class);
		job.setMapperClass(wineBuyerMapper.class);
		job.setReducerClass(wineBuyerReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setSortComparatorClass (IntWritableDecreasingComparator.class);
		FileInputFormat.addInputPath(job, inputFolder);
		FileOutputFormat.setOutputPath(job, outputFolder);
		System.exit(job.waitForCompletion(true)?0:1);
	}

	public static class IntWritableDecreasingComparator extends
			IntWritable.Comparator {
		public int compare (byte [] b1, int s1, int l1, byte [] b2, int s2, int l2)
		{
			return -super.compare (b1, s1, l1, b2, s2, l2);
		}
	}

	public static class wineBuyerMapper extends Mapper<Object, Text, IntWritable,Text>{
		public int curr_line = 0 ,idCounter=0;
		public void map(Object key, Text value, Context con) throws IOException, InterruptedException
		{
			if (curr_line != 0) {
				String[] line_tokens = value.toString().split(";");

				int mntWines;
				int diffAge = 2021 - Integer.parseInt(line_tokens[1]);
				String wntWine = line_tokens[9];

				if (wntWine.isEmpty())
					mntWines =0;
				else
					mntWines = Integer.parseInt(line_tokens[9]);

					if(mntWines >= (303.93 + 303.93 / 2))
					{
						con.write(new IntWritable(mntWines),
								new Text("ID:" + line_tokens[0] + " Age:" + String.valueOf(diffAge) + " Education:" + line_tokens[2] + " Marital_Status:" + line_tokens[3] + " Income:" + line_tokens[4] + " MntWines:" + line_tokens[9]));
					}
			}
			curr_line++;
		}
	}

	public static class wineBuyerReducer extends Reducer<IntWritable, Text, IntWritable, Text>
	{
		int counter=0;
		public void reduce(IntWritable key, Iterable<Text> values, Context con) throws IOException, InterruptedException
		{
			for (Text val : values)
			{
				counter++;
				con.write(new IntWritable(counter),val);
			}
		}
	}
}
