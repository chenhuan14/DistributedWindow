package nudt.pdl.stormwindow.topology;

import java.util.Map;
import java.util.Random;

import org.apache.commons.lang.Validate;

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
import clojure.lang.Util;
import nudt.pdl.stormwindow.event.Attribute;
import nudt.pdl.stormwindow.event.TupleEventType;
import nudt.pdl.stormwindow.operator.AbsWindowedOperator;
import nudt.pdl.stormwindow.operator.IRichOperator;
import nudt.pdl.stormwindow.operator.aggregation.AggregateSum;
import nudt.pdl.stormwindow.storm.StormBolt;
import nudt.pdl.stormwindow.storm.WindowedStormBolt;
import nudt.pdl.stormwindow.window.creator.WindowEviction;
import nudt.pdl.stormwindow.window.creator.WindowInfo;
import nudt.pdl.stormwindow.window.creator.WindowType;

class RandomLongSpout extends BaseRichSpout{
	  
	SpoutOutputCollector _collector;
	Random _rand;
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		_rand = new Random(System.currentTimeMillis());
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		Utils.sleep(100);
		long data = _rand.nextLong();
		_collector.emit(new Values(data));
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
		builder.setSpout("spout", new RandomLongSpout(), 1);
		
		WindowedStormBolt sumBolt = new AggregateSum();
		sumBolt.setOutputStream("sumResult");
		sumBolt.setOutputSchema(new TupleEventType("sumResult", new Attribute(Long.class, "result")));
		
		WindowInfo info = new WindowInfo(WindowType.LengthtBased, WindowEviction.Sliding, 10);
		sumBolt.setWindowInfo(info);
		
		
		builder.setBolt("sum", sumBolt, 1).shuffleGrouping("spout");
		builder.setBolt("print", new ConsolePrinter()).allGrouping("sum", "sumResult");
		
		 Config conf = new Config();
		 conf.setDebug(true);
		
		 LocalCluster cluster = new LocalCluster();
	     cluster.submitTopology("aggregateSum", conf, builder.createTopology());
	}

	
}
