import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class step1 {
	/**
	 * The Input:
	 *      Google 1gram database
	 *              n-gram /T year /T occurrences /T pages /T books
	 *              program   1991    3   2   1
	 *              program   1986    4   2   1
	 *
	 * The Output:
	 *      For each line from the input it creates a line with the word and its occurrences.
	 *
	 *              T n-gram /T occurrences
	 *              program 		3  
	 *              *		        4  
	 */
	private static class Map extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			//System.out.println(value);
			String[] strings = value.toString().split("\t");
			//System.out.println("length: "+strings.length);
			String w1 = strings[0];
			//System.out.println("the word :"+w1);
			//System.out.println("the year: "+strings[1]);
			if(!(w1.equals("*"))){
			int occur = Integer.parseInt(strings[2]);
			Text text = new Text();
			text.set(String.format("%s ",w1));
			Text text1 = new Text();
			Text text2=new Text();
			text2.set(String.format("*"));
			text1.set(String.format("%d",occur));
			//System.out.println("the output: "+text.toString()+" "+text1.toString());
			context.write(text,text1);
			context.write(text2,text1);
			}
		}

	}


	/**
	 * Input:
	 *      The input is the sorted output of the mapper.
	 *      Maybe output from different mappers.
	 *      Template:
	 *              T n-gram /T occurrences
	 *                program  		3  
	 *                program       4
	 *
	 * Output:
	 *      Combines all the same occurrences by key.
	 *              T n-gram   	T occurrences
	 *               program       		7
	 */
	private static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String w1 = key.toString();
			int sum_occ = 0;
			for (Text val : values) {
				sum_occ += Long.parseLong(val.toString());
			}
			Text newKey = new Text();
			newKey.set(w1);
			Text newVal = new Text();
			newVal.set("" + sum_occ);
//			System.out.println("the key: "+newKey+" the value: "+newVal);
			context.write(newKey, newVal);
		}
	}


	private static class PartitionerClass extends Partitioner<Text,Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions){
			return Math.abs(key.hashCode()) % numPartitions;
		}
	}

	public static void main(String[] args) throws Exception, ClassNotFoundException, InterruptedException  {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(step1.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(step1.PartitionerClass.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
		String output="/output1/";
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
		
	}
}





