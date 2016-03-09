package nudt.pdl.stormwindow.topology;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import nudt.pdl.stormwindow.ConsolePrinter;
import nudt.pdl.stormwindow.merge.SumMerge;
import nudt.pdl.stormwindow.operator.aggregation.AggregateSum;
import nudt.pdl.stormwindow.spout.GenerateLongWithBlackTuple;
import nudt.pdl.stormwindow.storm.WindowedStormBolt;
import nudt.pdl.stormwindow.window.creator.WindowEviction;
import nudt.pdl.stormwindow.window.creator.WindowInfo;
import nudt.pdl.stormwindow.window.creator.WindowType;







public class SumTopologyWithBlackTuple {
	
	public static void main(String[] args)
	{
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new GenerateLongWithBlackTuple(), 1);
		
		WindowedStormBolt sumBolt = new AggregateSum();	
		
		//子窗口大小和单窗口一致，有占位符
		WindowInfo info = new WindowInfo(WindowType.LengthtBased, WindowEviction.Sliding, 6,false);
		sumBolt.setWindowInfo(info);
//		sumBolt.setInputStream(Arrays.asList("longInput"));
		
		
		builder.setBolt("sum", sumBolt,3).directGrouping("spout");
		builder.setBolt("merge", new SumMerge()).shuffleGrouping("sum");
		builder.setBolt("print", new ConsolePrinter()).shuffleGrouping("merge");
		
		 Config conf = new Config();
		 conf.setDebug(false);
		
		 LocalCluster cluster = new LocalCluster();
	     cluster.submitTopology("aggregateSum", conf, builder.createTopology());
	}

	
}
