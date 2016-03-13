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
import nudt.pdl.stormwindow.FilePrinter;
import nudt.pdl.stormwindow.merge.SumMerge;
import nudt.pdl.stormwindow.operator.aggregation.AggregateSum;
import nudt.pdl.stormwindow.spout.GenerateLongWithBlackTuple;
import nudt.pdl.stormwindow.spout.GenerateLongWithBlackTupleFromFile;
import nudt.pdl.stormwindow.storm.WindowedStormBolt;
import nudt.pdl.stormwindow.util.Constant;
import nudt.pdl.stormwindow.window.creator.WindowEviction;
import nudt.pdl.stormwindow.window.creator.WindowInfo;
import nudt.pdl.stormwindow.window.creator.WindowType;







public class SumTopologyWithBlackTuple {
	
	public static final String outputFileName = Constant.OUTPUT_DIR+"sum_blackTupe_input10k_lengthwindow10.txt";
	
	public static void main(String[] args)
	{
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new GenerateLongWithBlackTupleFromFile(Constant.LONG_FILE_10K), 1);
		
		WindowedStormBolt sumBolt = new AggregateSum();	
		
		//子窗口大小和单窗口一致，有占位符
		WindowInfo info = new WindowInfo(WindowType.LengthtBased, WindowEviction.Sliding,10,false);
		sumBolt.setWindowInfo(info);
		
		builder.setBolt("sum", sumBolt,3).directGrouping("spout");
		builder.setBolt("merge", new SumMerge()).shuffleGrouping("sum");
		builder.setBolt("print", new FilePrinter(outputFileName)).shuffleGrouping("merge");
		
		 Config conf = new Config();
		 conf.setDebug(false);
		
		 LocalCluster cluster = new LocalCluster();
	     cluster.submitTopology("aggregateSum", conf, builder.createTopology());
//	     
	     Utils.sleep(10*1000);
	     cluster.shutdown();
	}

	
}
