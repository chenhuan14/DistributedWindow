package nudt.pdl.stormwindow.skyline;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import nudt.pdl.stormwindow.storm.WindowedStormBolt;

public class EagerSkyline extends WindowedStormBolt{
	
	private List<SkyTuple> localSkyline;
	
	private Map<Long,SkyTuple> localDatas; 
	
	

	/* (non-Javadoc)
	 * @see nudt.pdl.stormwindow.storm.WindowedStormBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		super.prepare(stormConf, context, collector);
		
		localSkyline = new LinkedList<>();
		localDatas = new TreeMap<Long,SkyTuple>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("isnew","rawdata"));
		
	}

	@Override
	public void process(Tuple[] newData, Tuple[] oldData) {
		// TODO Auto-generated method stub

		
		if(null != newData)
		{
			processNewData(newData);
		}
		
		if(null != oldData)
		{
			processOldData(oldData);
		}
		
	}
	
	private void processNewData(Tuple[] newData)
	{
		boolean isNewSkyline = true;
		long latestDominateTuple = 0;

		
		for(Tuple stormTuple : newData)
		{
			String rawData = stormTuple.getStringByField("rawdata");
			SkyTuple newTuple = Converter.buildTupleFromStr(rawData);
			ArrayList<SkyTuple> removedSkyline = new ArrayList<>();
			
			//倒序遍历
			for(SkyTuple skylineTuple : localSkyline)
			{
				int dominate = IsDominate.dominateBetweenTuples(newTuple, skylineTuple);
				if(dominate == 0)  //新元组支配旧元组
 				{
					removedSkyline.add(skylineTuple);
					isNewSkyline = true; 
				}
				else if(dominate == 1) //新元组被旧元组支配,立即跳出
				{
					isNewSkyline = false ;
					long tempID = skylineTuple.getTuple_ID();
					if(tempID > latestDominateTuple)
						latestDominateTuple = tempID;
				}
			}
			
			if(isNewSkyline)
			{
				localSkyline.removeAll(removedSkyline);
				for(SkyTuple tuple: removedSkyline)
				{
					sendToNextBolt(new Values(false,tuple.toString()));
				}
				
				localSkyline.add(newTuple);
				
				sendToNextBolt(new Values(true,newTuple.toString()));
				
				expungFromLocalData(newTuple);
				
			}
			else
			{
				long latestInLocalData = getLatesDominateTupe(newTuple);
				latestDominateTuple = latestDominateTuple > latestInLocalData ? 
										latestDominateTuple : latestInLocalData;
				localDatas.put(latestDominateTuple,newTuple);
			}
			
		}
	}
	

	private void processOldData(Tuple[] oldData)
	{
		for(Tuple stormTuple : oldData)
		{
			String rawData = stormTuple.getStringByField("rawdata");
			SkyTuple oldTuple = Converter.buildTupleFromStr(rawData);
			boolean isInSkyline = isInSkyline(oldTuple);
			if(isInSkyline)
			{
				localSkyline.remove(oldTuple);
				sendToNextBolt(new Values(false,oldTuple.toString()));
			}
			if(localDatas.containsKey(oldTuple.getTuple_ID()))
			{
				SkyTuple newSkyline = localDatas.remove(oldTuple.getTuple_ID());
				localSkyline.add(newSkyline);
				sendToNextBolt(new Values(true,newSkyline.toString()));
			}
		}
	}
	
	/*
	 * 提起删除候选集中被新元组支配的数据
	 */
	private void expungFromLocalData(SkyTuple newData)
	{
		ArrayList<Long> removed = new ArrayList<>();
		for(long key : localDatas.keySet())
		{
			if(IsDominate.dominate(newData, localDatas.get(key)))
			{
				removed.add(key);
			}
		}
		
		for(long l : removed)
		{
			localDatas.remove(l);
		}
	}
	
	/*
	 * 在候选集里获取最新支配新元组的数据
	 */
	private long getLatesDominateTupe(SkyTuple newData)
	{
		long latesIndex = 0;
		for(SkyTuple tuple : localDatas.values())
		{
			if(IsDominate.dominate(tuple, newData))
			{
				if(tuple.getTuple_ID() > latesIndex)
					latesIndex = tuple.getTuple_ID();
			}
		}
		return latesIndex;
	}
	
	private boolean isInSkyline(SkyTuple tuple)
	{
		for(SkyTuple skyline : localSkyline)
		{
			if(tuple.getTuple_ID() == skyline.getTuple_ID())
			{
				return true;
			}
		}
		return false;
	}

}
