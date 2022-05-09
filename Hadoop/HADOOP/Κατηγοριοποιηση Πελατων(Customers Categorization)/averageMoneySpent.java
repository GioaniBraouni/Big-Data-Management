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



public class averageMoneySpent{
	
	public static void main(String [] args) throws Exception
	{
		Configuration configuration =new Configuration();
		Path inputFolder = new Path(args[0]);
		Path outputFolder = new Path(args[1]);
		//Path finalOutputFolder = new Path(args[2]);

		Job job =new Job(configuration,"averageMoneySpent");
		job.setJarByClass(averageMoneySpent.class);

		job.setMapperClass(averageMoneySpentMapper.class);
		job.setReducerClass(averageMoneySpentReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, inputFolder);
		FileOutputFormat.setOutputPath(job, outputFolder);

		//job.setNumReduceTasks(1);
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);

		return;
	}

	public static class averageMoneySpentMapper extends Mapper<Object, Text, Text,DoubleWritable>{
		public int curr_line = 0;
		public void map(Object key, Text value, Context con) throws IOException, InterruptedException
		{
			if (curr_line != 0) 
			{	
				String[] line_tokens = value.toString().split(";");

				String wines = line_tokens[9];
				String fruits = line_tokens[10];
				String meat = line_tokens[11];
				String fish = line_tokens[12];
				String sweet = line_tokens[13];
				String gold = line_tokens[14];
				String dateInput = line_tokens[7];
				String income = line_tokens[4];

				double Wines,Fruits,Meat,Fish,Sweet,Gold,Income;

				if(wines.isEmpty())
					Wines = 0;
				else
					Wines = Double.parseDouble(wines);

				if(fruits.isEmpty())
					Fruits = 0;
				else
					Fruits = Double.parseDouble(fruits);

				if(meat.isEmpty())
					Meat = 0;
				else
					Meat = Double.parseDouble(meat);

				if(sweet.isEmpty())
					Sweet = 0;
				else
					Sweet = Double.parseDouble(sweet);

				if(fish.isEmpty())
					Fish = 0;
				else
					Fish = Double.parseDouble(fish);

				if(gold.isEmpty())
					Gold = 0;
				else
					Gold = Double.parseDouble(gold);

				double totalSpent = (Wines + Fruits + Meat + Fish + Sweet + Gold);

				con.write(new Text(""),new DoubleWritable(totalSpent));
			}
			curr_line++;
		}
	}
	
	public static class averageMoneySpentReducer extends Reducer<Text, DoubleWritable, Text, Text>
	{
		int count=0;
		double sum=0;
		double averageTotalSpent=0;

		public void reduce(Text key, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException
		{
			for(DoubleWritable value : values)
			{
				sum+=value.get();
				count++;
			}
			averageTotalSpent = sum/count;
			con.write(new Text("Average,Count ->"),new Text(averageTotalSpent +","+count));
		}
	}
}