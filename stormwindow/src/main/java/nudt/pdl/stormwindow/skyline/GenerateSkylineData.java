package nudt.pdl.stormwindow.skyline;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class GenerateSkylineData  extends BaseRichSpout{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5258581155587822943L;

	public static String inputFile = "source\\skylineInput\\rawdata_1_1K_5d.csv";
	
	private TopologyContext context;
    private	SpoutOutputCollector _collector;
	private BufferedReader reader;
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.context = context;
		_collector = collector;
		
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		
		
		
		String s = "";
		try {
			s = reader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(null != s && s.length() != 0)
		{
			_collector.emit(new Values(s.trim()));
		}
		
//		Utils.sleep(100);
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
		declarer.declare(new Fields("rawdata"));
		
	}

}
