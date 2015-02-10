//package demos.knn;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class KNNCombiner extends MapReduceBase implements Reducer<DoubleWritable, DoubleWritable, DoubleWritable, DoubleWritable>{
	private static int k = KNN.DEFAULT_K;
	private int selected = 0;
	
	public void configure(JobConf conf) {
		k = conf.getInt(KNN.K_CONF, k);
	}

	@Override
	public void reduce(DoubleWritable key, Iterator<DoubleWritable> values,
			OutputCollector<DoubleWritable, DoubleWritable> output, Reporter reporter)
					throws IOException {
		// only keep the top k scores
		while( selected < k && values.hasNext() ){
			output.collect(key, values.next());
			selected++;
		}
	}
}
