package nudt.pdl.stormwindow.skyline;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class SkylineMerge implements IRichBolt{
	
	
	private List<SkyTuple> datas;
	
	OutputCollector collector ;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		
		this.collector = collector;
		datas = new ArrayList<>();
		
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		
		boolean isNew = input.getBooleanByField("isnew");
		String rawData = input.getStringByField("rawdata");
		
		SkyTuple tuple = Converter.buildTupleFromStr(rawData);
		if(isNew)
			datas.add(tuple);
		else
			datas.remove(tuple);
		
		List<SkyTuple> skyline = getSkyline();
		collector.emit(new Values(skyline.size(),skyline));
				
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
		declarer.declare(new Fields("skylinecount","skylinearray"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	private List<SkyTuple> getSkyline()
	{	
		//定义一个节点为数组类型的链表，用于存放候选的Skyline点及最终结果
		LinkedList<SkyTuple> globalspList = new LinkedList<SkyTuple>();

		for(SkyTuple tuple: datas){
			if(globalspList.isEmpty())
				globalspList.add(tuple);
			else{
				int i = 0;
				boolean flag = true;
				while(flag){
					//调用支配关系测试函数，测试两个对象之间的支配关系
					int isDominate = IsDominate.dominateBetweenTuples(globalspList.get(i), tuple);
					switch(isDominate){
					case 0:					//tuple被支配，直接删去该对象
						flag = false;		//跳出while循环，读下一个对象
						break;
					case 1:					//tuple支配spList中的第i个对象，删去该对象，继续往后比较，进入下一次循环
						globalspList.remove(i);
						break;
					case 2:					//tuple与spList中的第i个对象互不支配，继续往后比较，进入下一次循环
						i++;				//比较对象变为链表中的下一个对象
						break;
					default:
						break;
					}
					if(i==globalspList.size()){	//如果比较到spList的最后一个对象而tuple仍然未被支配，则将其加入该链表的表尾
						globalspList.addLast(tuple);
						flag = false;		//结束while循环，读文件中的下一行
					}
				}
			}
		}
		return globalspList;
	}

}
