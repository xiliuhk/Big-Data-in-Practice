import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KNNReducer extends Reducer<Text, Text, Text, Text>{
	
	private static Vector<String> gold = new Vector<String>();
	private static int goldIndex = 0;
	
	@Override
	public void reduce(Text key, Iterable<Text> value,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		Vector<String> votes = new Vector<String>();
		while (value.iterator().hasNext()){
			votes.add(value.iterator().next().toString());
		}
		String correctness = "";
		if (!gold.get(goldIndex).equals(voteCounter(votes))){
			correctness = "incorrect";
		}else{
			correctness = "correct";
		}
		context.write(key, new Text(voteCounter(votes) + "	"+ correctness));
	}
	
	@Override
	public void setup(Reducer<Text, Text, Text, Text>.Context context){
		try {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			String goldPath = context.getConfiguration().get("TEST_GOLD");
	
			BufferedReader fileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(goldPath))));
			String line = fileReader.readLine();
			
			while(line != null){
				gold.add(line);
				line = fileReader.readLine();
			}
			fileReader.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String voteCounter(Vector<String> votes){
		HashMap<String, Integer> cnter = new HashMap<String, Integer>();
		for (String spec : votes){
			if (!cnter.containsKey(spec)){
				cnter.put(spec, 0);
			}else{
				cnter.put(spec, cnter.get(spec)+1);
			}
		}
		//get maximum count
		int maxCnt = 0;
		String maxSpec = "";
		for (String spec : cnter.keySet()){
			if (cnter.get(spec) > maxCnt){
				maxCnt = cnter.get(spec);
				maxSpec = spec;
			}
		}
		return maxSpec;
	}
	
}
