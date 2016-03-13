package nudt.pdl.stormwindow.skyline;

/**
 * @author Purple Wang
 * Jan 20, 2014
 */
public class SkyTuple {
	
	private long tuple_ID;			//the id of the uncertain tuple
	private double[] attr_array;	//the attributes array of the tuple
	
	
	//the constructor functions
	public SkyTuple(){}
	
	public SkyTuple(long id, double[] attrs){
		this.tuple_ID = id;
		this.attr_array = attrs;
	}

	
	//the getter and setter of each member variable
	public void setTuple_ID(long tuple_ID) {
		this.tuple_ID = tuple_ID;
	}
	public long getTuple_ID() {
		return tuple_ID;
	}


	
	public void setAttr_array(double[] atrr_array) {
		this.attr_array = atrr_array;
	}
	public double[] getAttr_array() {
		return attr_array;
	}
	
	//the toString approaches
	//重写toString()方法，将tuple对象按要求组织成String类型的字符串，包括元组ID和各维属性值ֵ
	public String toString(){
		String str = "";
		str += getTuple_ID();
		str += ",";
		str += Converter.arrayToString(getAttr_array());
		return str;
	}
	
	
	public boolean equals(Object obj)
	{
		boolean flag = false;
		if(obj instanceof SkyTuple)
		{
			SkyTuple tuple = (SkyTuple)obj;
			//简单判定两个Tuple ID相等则相等
			if(this.tuple_ID == tuple.getTuple_ID())
				flag = true;
		}
		
		return flag;
	}
	
}
