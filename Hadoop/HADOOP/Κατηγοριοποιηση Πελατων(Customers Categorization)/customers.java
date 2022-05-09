import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class customers{

	public static void main(String [] args) throws Exception
	{
		Configuration configuration =new Configuration();
		Path inputFolder = new Path(args[0]);
		Path outputFolder = new Path(args[1]);

		Job job2 = new Job(configuration, "clientCategorization");

		job2.setJarByClass(customers.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(LongWritable.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setMapperClass(clientCategorizationMapper.class);
		job2.setReducerClass(clientCategorizationReducer.class);

		//job2.setSortComparatorClass (IntWritableDecreasingComparator.class);

		FileInputFormat.addInputPath(job2, inputFolder);
		FileOutputFormat.setOutputPath(job2, outputFolder);

		job2.waitForCompletion(true);

		return;
	}

	public static class IntWritableDecreasingComparator extends
			IntWritable.Comparator {
		public int compare (byte [] b1, int s1, int l1, byte [] b2, int s2, int l2) {
			return -super.compare (b1, s1, l1, b2, s2, l2);
		}
	}

	public static class clientCategorizationMapper extends Mapper<Object, Text, Text,LongWritable>{
		public int curr_line = 0;
		public void map(Object key, Text value, Context con) throws IOException, InterruptedException
		{
			if (curr_line != 0)
			{
				String[] line_tokens = value.toString().split(";");
				String id = line_tokens[0];
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

				if(income.isEmpty())
					Income = 0;
				else
					Income = Double.parseDouble(income);

				if(!dateInput.isEmpty() && !id.isEmpty())
				{
					int Id = Integer.parseInt(id);
					double totalSpent = (Wines + Fruits + Meat + Fish + Sweet + Gold);
					String[] extractDate = dateInput.toString().split("/");

					if(totalSpent>=(605.79 + 605.79/2))
					{
						if(Income>69500) {
							try {
								SimpleDateFormat date = new SimpleDateFormat("yy");
								Date date1 = date.parse("21");
								Date date3 = date.parse(extractDate[2]);

								int result = date3.compareTo(date1);
								if (result == 0)
									con.write(new Text("Gold"), new LongWritable(Id));
								else
									con.write(new Text("Silver"), new LongWritable(Id));
							} catch (ParseException e) {
								e.printStackTrace();

							}
						}
					}
				}
			}
			curr_line++;
		}
	}

	public static class clientCategorizationReducer extends Reducer<Text, LongWritable, Text, Text>
	{
		public void reduce(Text key, Iterable<LongWritable> value, Context con) throws IOException, InterruptedException
		{
			ArrayList<Long> list_of_ids = new ArrayList<>();
			for(LongWritable id : value) {
				list_of_ids.add(id.get());
			}
			Collections.sort(list_of_ids);
			con.write(key,new Text(list_of_ids.toString()));
		}
	}
}