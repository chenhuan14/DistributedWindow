package nudt.pdl.stormwindow.topology;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import nudt.pdl.stormwindow.FilePrinter;
import nudt.pdl.stormwindow.merge.SumMerge;
import nudt.pdl.stormwindow.merge.SumMergeRoundRobin;
import nudt.pdl.stormwindow.operator.aggregation.AggregateSum;
import nudt.pdl.stormwindow.spout.GenerateLongWithBlackTuple;
import nudt.pdl.stormwindow.spout.GenerateLongWithRoundRobin;
import nudt.pdl.stormwindow.spout.GenerateLongWithRoundRobinFromFile;
import nudt.pdl.stormwindow.storm.WindowedStormBolt;
import nudt.pdl.stormwindow.util.Constant;
import nudt.pdl.stormwindow.window.creator.WindowEviction;
import nudt.pdl.stormwindow.window.creator.WindowInfo;
import nudt.pdl.stormwindow.window.creator.WindowType;


public class SumTopologyWithRoundRobin {
	
	public static final String outputFileName = Constant.OUTPUT_DIR+"sum_roundRobin_input10k_lengthwindow10.txt";
	
	public static void main(String[] args)
	{
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new GenerateLongWithRoundRobinFromFile(Constant.LONG_FILE_10K), 1);
		
		WindowedStormBolt sumBolt = new AggregateSum();	
		
		// 采用轮询方式，窗口被等价的切分为M份
		WindowInfo info = new WindowInfo(WindowType.LengthtBased, WindowEviction.Sliding, 10,true);
		sumBolt.setWindowInfo(info);
//		sumBolt.setInputStream(Arrays.asList("longInput"));
		
		
		builder.setBolt("sum", sumBolt,2).directGrouping("spout");
		builder.setBolt("merge", new SumMergeRoundRobin()).shuffleGrouping("sum");
		builder.setBolt("print", new FilePrinter(outputFileName)).shuffleGrouping("merge");
		
		 Config conf = new Config();
		 conf.setDebug(false);  
		
		 LocalCluster cluster = new LocalCluster();

	     cluster.submitTopology("aggregateSum", conf, builder.createTopology());
	     
	     
	     Utils.sleep(10*1000);
	     cluster.shutdown();
	}

	
}
