package nudt.pdl.stormwindow.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import nudt.pdl.stormwindow.datasplit.RoundRobinSpilt;
import nudt.pdl.stormwindow.skyline.EagerSkyline;
import nudt.pdl.stormwindow.skyline.GenerateSkylineData;
import nudt.pdl.stormwindow.skyline.SkylineMerge;
import nudt.pdl.stormwindow.skyline.SkylineOutPut;
import nudt.pdl.stormwindow.storm.WindowedStormBolt;
import nudt.pdl.stormwindow.window.creator.WindowEviction;
import nudt.pdl.stormwindow.window.creator.WindowInfo;
import nudt.pdl.stormwindow.window.creator.WindowType;

public class SkylineTopology {

	public static void main(String[] args)
	{
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new GenerateSkylineData());
		
		
		WindowedStormBolt skyline = new EagerSkyline();	
		
		//RoundRobin 平分窗口
		WindowInfo info = new WindowInfo(WindowType.LengthtBased, WindowEviction.Sliding,1000,true);
		skyline.setWindowInfo(info);
		
		builder.setBolt("split", new RoundRobinSpilt("skyline")).shuffleGrouping("spout");
		
		builder.setBolt("skyline", skyline,5).directGrouping("split");
		
		builder.setBolt("merge", new SkylineMerge()).shuffleGrouping("skyline");

		builder.setBolt("print", new SkylineOutPut()).shuffleGrouping("merge");
		
		 Config conf = new Config();
		 conf.setDebug(false);
		
		 LocalCluster cluster = new LocalCluster();
	     cluster.submitTopology("skyline", conf, builder.createTopology());
//	     
//	     Utils.sleep(10*1000);
//	     cluster.shutdown();
	}
}
