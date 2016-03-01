package nudt.pdl.stormwindow.topology;

import java.util.Map;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import nudt.pdl.stormwindow.operator.aggregation.AggregateSum;

import nudt.pdl.stormwindow.storm.WindowedStormBolt;
import nudt.pdl.stormwindow.window.creator.WindowEviction;
import nudt.pdl.stormwindow.window.creator.WindowInfo;
import nudt.pdl.stormwindow.window.creator.WindowType;

class RandomLongSpout extends BaseRichSpout{
	  
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	SpoutOutputCollector _collector;
	static long i = 0 ;
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		Utils.sleep(1000);
		_collector.emit(new Values(i++));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("long"));
	}
}

class ConsolePrinter extends BaseBasicBolt{

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		System.out.println("sum = " + input.getLong(0));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
}

public class SumTopology {
	
	public static void main(String[] args)
	{
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new RandomLongSpout(), 2);
		
		WindowedStormBolt sumBolt = new AggregateSum();	
		WindowInfo info = new WindowInfo(WindowType.LengthtBased, WindowEviction.Sliding, 2);
		sumBolt.setWindowInfo(info);
		
		
		builder.setBolt("sum", sumBolt, 1).shuffleGrouping("spout");
		builder.setBolt("print", new ConsolePrinter()).allGrouping("sum", "sumResult");
		
		 Config conf = new Config();
		 conf.setDebug(false);
		
		 LocalCluster cluster = new LocalCluster();
	     cluster.submitTopology("aggregateSum", conf, builder.createTopology());
	}

	
}
