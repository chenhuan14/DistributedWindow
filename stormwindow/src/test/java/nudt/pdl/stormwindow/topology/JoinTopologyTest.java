package nudt.pdl.stormwindow.topology;

import java.util.Map;
import java.util.Random;

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
import nudt.pdl.stormwindow.operator.join.InnerJoinCount;
import nudt.pdl.stormwindow.storm.WindowedJoinBolt;
import nudt.pdl.stormwindow.storm.WindowedStormBolt;
import nudt.pdl.stormwindow.window.creator.WindowEviction;
import nudt.pdl.stormwindow.window.creator.WindowInfo;
import nudt.pdl.stormwindow.window.creator.WindowType;



class ItemSpout extends BaseRichSpout{
	  
	
	SpoutOutputCollector _collector;
	
	Random random;
	
	public ItemSpout() {
		// TODO Auto-generated constructor stub
		random = new Random(System.currentTimeMillis());
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		Utils.sleep(1000);
		_collector.emit("left",new Values(random.nextInt() % 10, random.nextInt()%100));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream("left", new Fields("type","price"));
	}
}


class SoldSpout extends BaseRichSpout{
	  
	
	SpoutOutputCollector _collector;
	
	Random random;
	
	public SoldSpout() {
		// TODO Auto-generated constructor stub
		random = new Random(System.currentTimeMillis());
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		Utils.sleep(1000);
		_collector.emit("right",new Values(random.nextInt() % 10, random.nextInt()%20));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream("right",new Fields("type","number"));
	}
}

class Printer extends BaseBasicBolt{

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		System.out.println("JoinCount = " + input.getValueByField("matchcount"));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
}

public class JoinTopologyTest {

	public static void main(String[] args)
	{
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("item", new ItemSpout());
		
		builder.setSpout("sold", new SoldSpout());
		
	
		WindowedJoinBolt joinOperator = new InnerJoinCount();
		WindowInfo leftWindow = new WindowInfo(WindowType.LengthtBased, WindowEviction.Sliding, 5);
		joinOperator.setLeftWindow(leftWindow);
		
		WindowInfo rightWindow = new WindowInfo(WindowType.LengthtBased, WindowEviction.Sliding, 20);
		joinOperator.setRightWindow(rightWindow);
		
		joinOperator.setLeftStreamName("left");
		joinOperator.setRightStreamName("right");
		
		
		builder.setBolt("join", joinOperator).shuffleGrouping("item", "left")
											.shuffleGrouping("sold","right");
		builder.setBolt("print", new Printer()).allGrouping("join");
		
	
		
		 Config conf = new Config();
		 conf.setDebug(false);
		
		 LocalCluster cluster = new LocalCluster();
	     cluster.submitTopology("Join", conf, builder.createTopology());
	}
	
}
