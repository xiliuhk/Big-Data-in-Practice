//package demos.knn;

import java.io.IOException;

import ml.data.CSVDataReader;
import ml.data.Example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class KNNMapper extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, DoubleWritable> {
	private Example testExample;
	private static DoubleWritable label = new DoubleWritable();
	private static DoubleWritable distance = new DoubleWritable();
	
	public void configure(JobConf conf){
		// set the test example we're trying to classify
		testExample = CSVDataReader.parseCSVExample(conf.get(KNN.TEST_EXAMPLE_CONF));
	}
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<DoubleWritable, DoubleWritable> output, Reporter reporter) throws IOException {
		Example other = CSVDataReader.parseCSVExample(value.toString());
		label.set(other.getLabel());
		distance.set(-getSimilarity(testExample, other)); // larger values will show up first
		output.collect(distance, label);
	}
		
	/**
	 * Return the cosine similarity of two examples.  Larger similarities 
	 * mean the two examples are more similar
	 */
	private static double getSimilarity(Example e1, Example e2){
		double dist = 0.0;
			
		// for now (though this isn't correct for sparse data sets) assume features set of e1 = feature set of e2
		for( Integer featureNum: e1.getFeatureSet() ){
			double diff = e1.getFeature(featureNum) - e2.getFeature(featureNum);
			dist += diff*diff;
		}
				
		return Math.sqrt(dist);
	}
}
