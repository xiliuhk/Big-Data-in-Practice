import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KNN_Driver {
	
	public static final String TEST_PATH = "iris_test_data.csv";
	public static final String TEST_GOLD = "iris_test_label.csv";
	public static final String TRAIN_PATH = "iris_train.csv";
	public static final String OUTPUT_PATH = "";
	public static final String K = "K";
	public static Vector<Iris> testSet;	

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("TRAIN_PATH", args[0]+TRAIN_PATH);
		conf.set("TEST_PATH", args[0]+TEST_PATH);
		conf.set("TEST_GOLD", args[0]+TEST_GOLD);
		conf.set("OUTPUT_PATH", args[1]);
		conf.setInt(K, Integer.parseInt(args[2]));
		
		Job job = Job.getInstance(conf, "KNN-Iris");
		
		job.setJarByClass(KNN_Driver.class);
		
	    job.setMapperClass(KNNMapper.class);
	    job.setCombinerClass(KNNCombiner.class);
	    job.setReducerClass(KNNReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
//	    job.setMapOutputKeyClass(Text.class);
//	    job.setMapOutputValueClass(MapValue.class);
//	    
	    FileInputFormat.addInputPath(job, new Path(args[0]+TRAIN_PATH));	    
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	    
	}
}
