import java.io.*;
import java.util.*;
import java.text.*;
import java.lang.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRank
{
	static int n;
	static double x[] = new double[n];

	

	public static class MatrixMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		public MatrixMapper()
		{
			
		}
		
		@Override
		protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			String dimension = conf.get("dimension");
			int dim = Integer.parseInt(dimension);
			
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens())
			{
				String line = itr.nextToken();
				if(line.isEmpty())
					continue;
				String[] entry = line.split(",");
				String sKey = "";	
				String mat = entry[0].trim();
				
				String row, col;
				
				if(mat.matches("A"))
				{
					row = entry[1].trim();
					
						sKey = row + ", 0";
						context.write(new Text(sKey),value);
					
				}
				
				else if(mat.matches("x"))
				{
					for (int i =0; i < dim ; i++)
					{
						sKey = i + ", 0";
						System.out.println("Mapping: " + sKey + " " + value.toString());
						context.write(new Text(sKey),value);
					}
				}
			}
		}
	}

	public static class MatrixReducer extends Reducer<Text, Text, Text, DoubleWritable>
	{
		public MatrixReducer()
		{
			
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
		{
			Configuration conf = context.getConfiguration();
			String dimension = conf.get("dimension");

			int dim = Integer.parseInt(dimension);

			double[] row = new double[dim];
			double[] col = new double[dim];
			
			for(Text val : values)
			{
				String[] entries = val.toString().split(",");
				if(entries[0].matches("A"))
				{
					int index = Integer.parseInt(entries[2].trim());
					row[index] = Double.parseDouble(entries[3].trim());
				}
				if(entries[0].matches("x"))
				{
					int index = Integer.parseInt(entries[1].trim());
					col[index] = Double.parseDouble(entries[3].trim());
				}
			}
			
			double total = 0;
			for(int i = 0 ; i < dim; i++)
			{
				total += row[i]*col[i];
			}
			Text key1 = new Text();
			key1.set(key.toString());
			key.set("x, " + key.toString() + ", ");
			System.out.println("Reducing: " + key.toString() + " " + total);
			context.write(key, new DoubleWritable(total));
			int r = Integer.parseInt(key1.toString().split(",")[0].trim());
			x[r] = total;
		}
	}

	
	public static void main(String[] args) throws Exception
	{
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
		Date date = new Date();
		String timestamp = dateFormat.format(date);
		Scanner in = new Scanner(System.in);
		System.out.print("Enter the dimension of the Adjacency matrix:");
		n=in.nextInt();
		System.out.print("Enter the number of Iterations:");
		noOfIterations=in.nextInt();


		Process r, t;
		int count = 0;
		do
		{
			Configuration conf = new Configuration();
			conf.set("dimension", Integer.toString(n)); // set the matrix dimension here
			Job job = Job.getInstance(conf, "matrix multiplication");
			job.setJarByClass(PageRank.class);
			job.setMapperClass(MatrixMapper.class);
			job.setReducerClass(MatrixReducer.class);
			job.setMapOutputKeyClass(Text.class); 
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(job, new Path("hdfs://localhost:50070/user/hduser/pagerank/temp-"+count+"/x.csv"));
			FileInputFormat.addInputPath(job, new Path("hdfs://localhost:50070/user/hduser/pagerank/temp-"+count+"/A.csv"));
			FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:50070/user/hduser/pagerank/temp-"+(count+1)));
			System.out.println("Running the MapReduce job. Please be patient...");
			if (job.waitForCompletion(true))
			{
				System.out.println("Iteration " + count + " has been completed\n");
			}
			else
			{
				System.out.println("The MapReduce job failed !!");
				System.exit(1);
			}

			t = Runtime.getRuntime().exec("hadoop fs -cp /user/hduser/pagerank/temp-"+count+"/A.csv /user/hduser/pagerank/temp-"+(count+1)+"/A.csv");
			t.waitFor();
			r = Runtime.getRuntime().exec("hadoop fs -mv /user/hduser/pagerank/temp-"+(count+1)+"/part-r-00000 /user/hduser/pagerank/temp-"+(count+1)+"/x.csv");
			r.waitFor();

			count++;

		}while(count <= noOfIterations);


		
		double sum = 0;
		for(int i=0; i<n; i++)
		{
			sum += x[i];
		}
		

		System.out.println("Final ranks of  websites are as follows:");
		for(int i=0; i<n; i++)
		{
			System.out.println("Page " + (i+1) + " : " + (x[i]/sum));
		}
		
		Process u = Runtime.getRuntime().exec("hadoop fs -rm -R /user/hduser/pagerank/temp-*");
		u.waitFor();
	}
}
