import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import java.io.BufferedReader;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.TransferManager;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;

public class step5 {
	/**
	 * The input:
	 *      Combines all the occurrences with the same key.
	 *             T n-gram /T occurrences
	 *            program is good program  is	5  
	 *            program is good is good		4            
	 * The Output:
	 *               T n-gram /T occurrences
	 *               program is good program  is	5  
	 *               program is good is good		4   
	 */
	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			String[] strings = value.toString().split("\t");
			String[] words1 = strings[0].split(" ");
			String w1 = words1[0];
			String w2 = words1[1]; 
			String w3= words1[2];
			String[]words2=strings[1].split(" ");
			int occur =0;
			Text text = new Text();
			text.set(String.format("%s %s %s ",w1,w2,w3));
			if(words2.length>1){
				occur= Integer.parseInt(words2[2]) ;
				Text text2=new Text();
				String pair=words2[0]+" "+words2[1];
				text2.set(String.format("%s %d ",pair,occur));
				context.write(text, text2);
				//	System.out.println("3: "+text.toString()+" 2: "+text2.toString());
			}
			else{
				occur= Integer.parseInt(strings[1]) ;
				Text text1 = new Text();
				text1.set(String.format("%d ",occur));
				//System.out.println("the text "+text.toString()+" "+text1.toString());
				context.write(text ,text1);
			}		

		}
		//		}

	}
	/**
	 * The input:
	 *      T n-gram /T occurrences
	 *      program is good program is	5  
	 *      program is good is good		4
	 *    	program is good 			4  
	 *		OR the output of step 1.    
	 * The Output:
	 *               T n-gram /T prob
	 *               program is good   0.6
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public static Long c0=new  Long(0);
		public static HashMap <String, Integer> map= new HashMap<String,Integer>(); 
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {			
			String[] strings = key.toString().split(" ");
			String w1 = strings[0];
			String w2 = strings[1];
			String w3= strings[2];
			//System.out.println("w1: "+w1+" w2: "+w2+" w3: "+w3);
			Integer N3=0;
			Integer N2=0;
			Integer N1=0;
			Integer C1=0;
			Long C0= new Long(0);
			Double k2=0.0;
			Double k3=0.0;
			Integer C2=0;
			Double prob=0.0;
			Text newKey = new Text();
			Text newVal = new Text();
			N1=map.get(w3);
			C1=map.get(w2);
		
			for (Text val : values) {
				String[] s=val.toString().split(" ");
				if(s.length<2){
					N3=(int) Long.parseLong(s[0]);
					k3=(Math.log(N3+1)+1)/(Math.log(N3+1)+2);
				}
				else{
					if(s[0].equals(w1)){	
						C2=(int) Long.parseLong(s[2]);
					}
					else{
						N2=(int) Long.parseLong(s[2]);
						k2=(Math.log(N2+1)+1)/(Math.log(N2+1)+2);
					}
				}
				if(C1!=null&&N1!=null){
					prob=(k3*(N3/C2))+((1-k3)*k2*(N2/C1))+((1-k3)*(1-k2)*(N1/C0));
					newKey.set(String.format("%s %s %s ",w1,w2,w3));
					newVal.set(String.format("%s ",prob));
					context.write(newKey, newVal);
				}
				else{
					System.out.println("the output is null");
				}
			}

		}

		public void setup(Reducer.Context context) throws IOException {  
			FileSystem fileSystem = FileSystem.get(context.getConfiguration());
			RemoteIterator<LocatedFileStatus> it=fileSystem.listFiles(new Path("/output1"),false);
			while(it.hasNext()){
				LocatedFileStatus fileStatus=it.next();
				if (fileStatus.getPath().getName().startsWith("part")){
					FSDataInputStream InputStream = fileSystem.open(fileStatus.getPath());
					BufferedReader reader = new BufferedReader(new InputStreamReader(InputStream, "UTF-8"));
					String line=null;
					String[] ones;
					while ((line = reader.readLine()) != null){
						ones = line.split("\t");
						if(ones[0].equals("*")){
							c0=Long.parseLong(ones[1]);
						}
						else{
						map.put(ones[0], (int) Long.parseLong(ones[1]));
						}
					}
					reader.close();
				}
			}
		}
	}

	//		private void loadTheOnes() throws IOException {
	//			Configuration conf = new Configuration();
	//			FileSystem local=FileSystem.getLocal(conf);
	//			//  FileSystem local = org.apache.hadoop.fs.FileSystem.getLocal(conf);
	//			Path inDir = new Path("/output1/");
	//			//for (File inFile : inDir.listFiles()) {
	//			for (FileStatus inFile : local.listStatus(inDir)) {
	//				//InputStream instream = new FileInputStream(inFile);
	//				InputStream instream = local.open(inFile.getPath());
	//				BufferedReader reader = new BufferedReader(
	//						new InputStreamReader(instream));
	//				String line=null;
	//				while ((line = reader.readLine()) != null){
	//					String[] one= line.split("\t");
	//					map.put(one[0],(int) Long.parseLong(one[1]));
	//				}
	//				reader.close();
	//			}
	//
	//		}

	//	for (File file : listOfFiles) {
	//		if(file.getName().startsWith("part")){
	//			FileInputStream fileInputStream= new FileInputStream(file);
	//			BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));
	//			String line=null;
	//			while ((line = reader.readLine()) != null){	
	//			}
	//			reader.close();
	//		}
	//	}
	//}



	private static class myPartitioner extends Partitioner<Text, Text>{
		@Override
		public int getPartition(Text key, Text value, int numPartitions){
			//System.out.println(key);
			return Math.abs(key.hashCode()) % numPartitions;
		}
	}


	public static void main(String[] args) throws Exception {
		//System.out.println(args[1]);
		//System.out.println(args[2]);
		System.out.println("new one 2");
		Configuration conf = new Configuration();
		//	conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		//	conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		Job job = Job.getInstance(conf);
		job.setJarByClass(step5.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(step5.myPartitioner.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//		FileSystem local=FileSystem.getLocal(conf);
		//		//  FileSystem local = org.apache.hadoop.fs.FileSystem.getLocal(conf);
		//		Path inDir = new Path("/output/1/");
		//		//for (File inFile : inDir.listFiles()) {
		//		for (FileStatus inFile : local.listStatus(inDir)) {
		//			//InputStream instream = new FileInputStream(inFile);
		//			InputStream instream = local.open(inFile.getPath());
		//			BufferedReader reader = new BufferedReader(
		//					new InputStreamReader(instream));
		//			String line=null;
		//			while ((line = reader.readLine()) != null){
		//				System.out.println(line);
		//			}
		//			reader.close();
		//		}
		//		//job.setInputFormatClass(TextInputFormat.class);
		//now we pass all the combained pair
		String input1="/output4/";
		String input2="/output3/";
		//String output="/output5/";
		MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class);
		//now we pass all the counted 3gram
		MultipleInputs.addInputPath(job, new Path(input2), TextInputFormat.class);	
		//output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}

}
