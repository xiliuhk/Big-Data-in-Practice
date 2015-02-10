
public class Iris {
	public double sepalLen;
	public double sepalWid;
	public double petalLen;
	public double petalWid;
	public String species;
	
	public Iris(String sepLen, String sepWid, String petLen, String petWid, String spec){
		this.sepalLen = Double.parseDouble(sepLen);
		this.sepalWid = Double.parseDouble(sepWid);
		this.petalLen = Double.parseDouble(petLen);
		this.petalWid = Double.parseDouble(petWid);
		this.species = spec;
	}
	
	public Iris(String sepLen, String sepWid, String petLen, String petWid){
		this.sepalLen = Double.parseDouble(sepLen);
		this.sepalWid = Double.parseDouble(sepWid);
		this.petalLen = Double.parseDouble(petLen);
		this.petalWid = Double.parseDouble(petWid);
		this.species = "";
	}
	
	
	public void classifyIris(String spec){
		this.species = spec;
	}
	
	@Override
	public String toString(){
		String str = "";
		str += Double.toString(this.sepalLen) +","+ Double.toString(this.sepalWid) +",";
		str += Double.toString(this.petalLen) + "," + Double.toString(this.petalWid);
		if (!this.species.equals("")){
			str +=","+this.species;	
		}
		return str;
	}
}
