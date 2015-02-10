import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MapValue implements Writable, Comparable<MapValue>{
	String species;
	double distance; 
	public MapValue(String spec, double dis){
		this.species = spec;
		this.distance = dis;
	}
	
	@Override
	public int compareTo(MapValue o) {
		if (this.distance > o.distance){
			return 1;
		}else{
			return -1;
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.species = in.readUTF();
		this.distance = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(species);
		out.writeDouble(distance);
	}
	
	
}
