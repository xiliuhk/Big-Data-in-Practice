import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KNNCombiner extends Reducer<Text, Text, Text, Text>{
	
	private int k = 0;
	private int selected = 0;
	
	@Override
	public void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		this.k = context.getConfiguration().getInt("K", 5);
		
		TreeMap<Double, Text> tmpList = new TreeMap<Double, Text>();
		
		while(values.iterator().hasNext()){
			String[] tmp = values.iterator().next().toString().split(",");
			tmpList.put(Double.parseDouble(tmp[1]), new Text(tmp[0]));	
		}
		int size = Math.min(this.k, tmpList.size());
		for (double dist : tmpList.keySet()){
			context.write(key, tmpList.get(dist));
			selected++;
			if (selected == size){
				break;
			}
		}
//		while(selected < size){
//			context.write(key, tmpList.get(selected));
//			selected++;
//		}
		
	}
}
