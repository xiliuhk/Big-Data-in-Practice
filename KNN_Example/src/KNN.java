//package demos.knn;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class KNN {
	public static final int DEFAULT_K = 5;
	
	public static final String K_CONF = "k";
	public static final String TEST_EXAMPLE_CONF = "testexample";
	
	public static void run(String input, String output){
		JobConf conf = new JobConf(KNN.class);
		conf.setJobName("KNN");
		
		// set the test example, could read this from a file, but we'll keep it simple
		conf.set(TEST_EXAMPLE_CONF, "0,0,0,0,1,0,1");
		
		// set k
		conf.setInt(K_CONF, 5);
		
		conf.setOutputKeyClass(DoubleWritable.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setMapOutputKeyClass(DoubleWritable.class);
		conf.setMapOutputValueClass(DoubleWritable.class);
		
		conf.setMapperClass(KNNMapper.class);
		conf.setCombinerClass(KNNCombiner.class);
		conf.setReducerClass(KNNReducer.class);
		
		// specify input and output dirs
	    FileInputFormat.addInputPath(conf, new Path(input));
	    FileOutputFormat.setOutputPath(conf, new Path(output));
		
	    conf.setNumReduceTasks(1);
	    
	    JobClient client = new JobClient();
	   	    
	    client.setConf(conf);
	    try {
	      JobClient.runJob(conf);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	}
	
	public static void main(String[] args) {
		if( args.length != 2 ){
			System.err.println("KNN <train_dir> <output_dir>");
		}else{
			run(args[0], args[1]);
		}
	}
}