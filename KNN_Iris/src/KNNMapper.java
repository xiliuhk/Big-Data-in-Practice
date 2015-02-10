import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KNNMapper extends Mapper<Object, Text, Text, Text> {
	private Vector<Iris> testSet = new Vector<Iris>();
	
	@Override
	public void setup(Mapper<Object, Text, Text, Text>.Context context){
		try {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			String testPath = context.getConfiguration().get("TEST_PATH");
	
			BufferedReader fileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(testPath))));
			String line = fileReader.readLine();
			
			while(line != null){
				String[] tokens = line.split(",");
				Iris tmp = new Iris(tokens[0], tokens[1], tokens[2], tokens[3]);
				testSet.add(tmp);
				line = fileReader.readLine();
			}
			fileReader.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void map(Object key, Text value,
			Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		//System.err.println("loading test sets...");
		String[] trainInsts = value.toString().split(",");
		Iris trainInst = new Iris(trainInsts[0], trainInsts[1], trainInsts[2], trainInsts[3], trainInsts[4] );
		double dist; 
		String spec;
		for (Iris testInst : testSet){
			//dist = calculateDist(trainInst, testInst);
			dist = calculateCosDist(trainInst, testInst);
			spec = trainInst.species;
			context.write(new Text(testInst.toString()), new Text(spec+","+dist));
		}
	}
	
		
	/**
	 * Return the cosine similarity of two examples.  Larger similarities 
	 * mean the two examples are more similar
	 */
	private static double calculateDist(Iris trainInst, Iris testInst){
		
		double dist = 0.0;			
		
		double d1 = trainInst.petalLen - testInst.petalLen;
		double d2 = trainInst.petalWid - testInst.petalWid;
		double d3 = trainInst.sepalLen - testInst.sepalLen;
		double d4 = trainInst.sepalWid - trainInst.sepalWid;
		
		dist = Math.sqrt(d1*d1 + d2*d2 + d3*d3 + d4*d4);
		
		return dist;
	}
	
	private static double calculateCosDist(Iris trainInst, Iris testInst){
			
			double dist = 0.0;			
			
			double d1 = trainInst.petalLen * testInst.petalLen;
			double d2 = trainInst.petalWid * testInst.petalWid;
			double d3 = trainInst.sepalLen * testInst.sepalLen;
			double d4 = trainInst.sepalWid * trainInst.sepalWid;
			
			dist = (d1+d2+d3+d4)/(normCal(trainInst)* normCal(testInst));
			
			return dist;
	}
	
	private static double normCal(Iris inst){
		double d1 = inst.petalLen * inst.petalLen;
		double d2 = inst.petalWid * inst.petalWid;
		double d3 = inst.sepalLen * inst.sepalLen;
		double d4 = inst.sepalWid * inst.sepalWid;
		return Math.sqrt(d1+d2+d3+d4);	
	}

	
}
