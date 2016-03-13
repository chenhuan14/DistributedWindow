package nudt.pdl.stormwindow;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;

import backtype.storm.tuple.Tuple;

public class FilePrinter implements IRichBolt{

	private BufferedWriter out = null; 
	private String outputFileName;
	
	
	
	public FilePrinter(String outputStringName) {
		// TODO Auto-generated constructor stub
		this.outputFileName = outputStringName;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		
		
		try {
			File file = new File(outputFileName);
			if(!file.exists())
				file.createNewFile();
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, false)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		System.out.println("sum = " + input.getLong(0));
		
		try {
			out.write(input.getLong(0) + "\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		if(out != null)
		{
			try {
				out.flush();
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
			
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
