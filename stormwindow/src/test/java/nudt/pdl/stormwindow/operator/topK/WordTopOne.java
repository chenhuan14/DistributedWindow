package nudt.pdl.stormwindow.operator.topK;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import nudt.pdl.stormwindow.storm.WindowedStormBolt;

public class WordTopOne extends WindowedStormBolt{
	

	private Map<String,Integer> wordCount ;

	
	public WordTopOne() {
		wordCount =  new HashMap<>();
	}

	@Override
	public void process(Tuple[] newData, Tuple[] oldData) {
		
		processNewData(newData);
		processOldData(oldData);
		String topOne = findTopOne();
		sendToNextBolt(new Values(topOne,wordCount.get(topOne)));
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("word","count"));
	}
	
	private void processNewData(Tuple[] newData)
	{
		for(Tuple t : newData)
		{
			String word = t.getString(0);
			int count = t.getInteger(1);
			if(wordCount.containsKey(word))
				wordCount.put(word, wordCount.get(word)+count);
			else
				wordCount.put(word, count);
		}
	}
	
	private void processOldData(Tuple[] oldData)
	{
		for(Tuple t : oldData)
		{
			String word = t.getString(0);
			int count = t.getInteger(1);
			if(wordCount.containsKey(word))
				wordCount.put(word, wordCount.get(word)-count);
		}
	}
	
	private String findTopOne()
	{
		int max = 0;
		String topOne = "";
		for(String key : wordCount.keySet())
		{
			int count = wordCount.get(key);
			if(count > max)
			{
				max = count;
				topOne = key;
			}
		}
		
		return topOne;
	}

}
