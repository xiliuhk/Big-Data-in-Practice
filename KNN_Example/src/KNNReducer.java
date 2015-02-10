//package demos.knn;

import java.io.IOException;
import java.util.Iterator;

import ml.data.CSVDataReader;
import ml.utils.HashMapCounter;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class KNNReducer extends MapReduceBase implements Reducer<DoubleWritable, DoubleWritable, DoubleWritable, DoubleWritable>{
	private static int k = KNN.DEFAULT_K;
	private int count = 0;
	private HashMapCounter<Double> labelCounts = new HashMapCounter<Double>();
	
	public void configure(JobConf conf) {
		k = conf.getInt(KNN.K_CONF, k);
	}
	
	// this reducer only works if there is a single reducer!
	// make sure to set this in the driver function
	@Override
	public void reduce(DoubleWritable key, Iterator<DoubleWritable> values,
			OutputCollector<DoubleWritable, DoubleWritable> output, Reporter reporter)
			throws IOException {
		while(count < k && values.hasNext()){
			labelCounts.increment(values.next().get());
			count++;
			
			if( count == k ){
				// record the value... we're done!
				double maxLabel = getMax(labelCounts);
				
				output.collect(new DoubleWritable(maxLabel), new DoubleWritable(labelCounts.get(maxLabel)));
			}
		}
	}
	
	private static double getMax(HashMapCounter<Double> counts){
		double maxLabel = 0.0;
		double maxCount = -Double.MAX_VALUE;
		
		for( Double label: counts.keySet()){
			if( counts.get(label) > maxCount ){
				maxCount = counts.get(label);
				maxLabel = label;
			}
		}
		
		return maxLabel;
	}
}
