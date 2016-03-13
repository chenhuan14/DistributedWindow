package nudt.pdl.stormwindow.skyline;

/**
 * @author Purple Wang
 * Jan 20, 2014
 */
public class Converter {
	
	/**
	 * arrayToString方法，将array对象转换成String类型，且每个维度之间用","隔开
	 * @param array 待转换的数组，为double类型
	 * @return 返回值为转换后的结果，为String类型
	 */
	public static String arrayToString(double[] array){
		int len=array.length;
		String str = "";
		for(int i=0; i<len; i++){
			str+=array[i];
			if(i<len-1)
				str+=",";
		}
		return str;
	}
	
	/**
	 * strToArray方法，将读入的一行数据元组按","分割开，并将每一个划分存入String数组中。
	 * 最后，将String数组转换成double数组
	 * @param str 待转换的字符串
	 * @return
	 */
	public static double[] strToArray(String str){
		String[] mark =  str.split(",");
		int dim = mark.length;
		double[] array = new double[dim];
		for(int i=0; i<mark.length; i++){
			array[i] = Double.parseDouble(mark[i]);
			//System.out.println(mark[i]);
		}
		return array;
	}
	
	/**
	 * buildTupleFromStr方法，根据一串字符串拆分信息构造Tuple对象
	 * @param str 待处理的字符串
	 * @return 构造成的Tuple对象
	 */
	public static SkyTuple buildTupleFromStr(String str){
		
		str = str.trim();
		
		//按照“，”将字符串str切分成若干段并存储在数组mark[]中
		//其中第一段为id，剩余若干段为各维度属性值
		String[] mark = str.split(",");
	
		long id = Long.parseLong(mark[0]);
		double[] attrs = new double[mark.length - 1];
		for(int i=1; i<mark.length; i++){
			attrs[i-1] = Double.parseDouble(mark[i]);
		}
		
		//根据抽取的信息，构造一个Tuple对象
		SkyTuple tuple = new SkyTuple(id, attrs);
		return tuple;
	}

	
}
