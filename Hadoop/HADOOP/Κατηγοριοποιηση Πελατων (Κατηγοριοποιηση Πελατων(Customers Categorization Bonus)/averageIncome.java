import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;



public class averageIncome{
	
	public static void main(String [] args) throws Exception
	{
		Configuration configuration =new Configuration();
		Path inputFolder = new Path(args[0]);
		Path outputFolder = new Path(args[1]);
		//Path finalOutputFolder = new Path(args[2]);

		Job job =new Job(configuration,"averageMoneySpent");
		job.setJarByClass(averageIncome.class);

		job.setMapperClass(averageIncomeMapper.class);
		job.setReducerClass(averageIncomeReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, inputFolder);
		FileOutputFormat.setOutputPath(job, outputFolder);

		//job.setNumReduceTasks(1);
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);

		return;
	}

	public static class averageIncomeMapper extends Mapper<Object, Text, Text,DoubleWritable>{
		public int curr_line = 0;
		public void map(Object key, Text value, Context con) throws IOException, InterruptedException
		{
			if (curr_line != 0) 
			{	
				String[] line_tokens = value.toString().split(";");

				String income = line_tokens[4];

				double Income;

				if(income.isEmpty())
					Income = 0;
				else
					Income = Double.parseDouble(income);

					con.write(new Text(""),new DoubleWritable(Income));
			}
			curr_line++;
		}
	}
	
	public static class averageIncomeReducer extends Reducer<Text, DoubleWritable, Text, Text>
	{
		int count=0;
		double sum=0;
		double averageIncome=0;
		public void reduce(Text key, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException
		{
			for(DoubleWritable value : values)
			{
				sum+=value.get();
				count++;
			}
			averageIncome = sum/count;
			con.write(new Text("Average,Count ->"),new Text(averageIncome +","+count));
		}
	}
}