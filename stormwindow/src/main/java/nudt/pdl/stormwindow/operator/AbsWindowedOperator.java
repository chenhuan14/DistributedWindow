package nudt.pdl.stormwindow.operator;

import java.util.List;
import java.util.Map;

import com.esotericsoftware.minlog.Log;

import nudt.pdl.stormwindow.event.IEvent;
import nudt.pdl.stormwindow.event.IEventType;
import nudt.pdl.stormwindow.event.TupleEvent;
import nudt.pdl.stormwindow.exception.StreamingException;
import nudt.pdl.stormwindow.exception.StreamingRuntimeException;
import nudt.pdl.stormwindow.output.OutputStorm;
import nudt.pdl.stormwindow.output.OutputType;
import nudt.pdl.stormwindow.storm.IEmitter;
import nudt.pdl.stormwindow.view.FirstLevelStream;
import nudt.pdl.stormwindow.window.IWindow;
import nudt.pdl.stormwindow.window.creator.WindowInfo;

public abstract class AbsWindowedOperator extends AbsOperator implements IProcessor {

	
	private WindowInfo windowInfo = new WindowInfo();
	private FirstLevelStream firstStream = new FirstLevelStream();
	private IWindow window;
	
	private IEventType outputSchema;
    
    private String outputStreamName;
    
    private List<String> inputStreams;
    
    private Map<String, IEventType> inputSchemas;

    
	
	public void setWindowInfo(WindowInfo info)
	{
		this.windowInfo = info;
	}
	
	public WindowInfo getWindowInfo()
	{
		return this.windowInfo;
	}

	@Override
	public void initialize() throws StreamingException {
		
		
	}
	
	@Override
	public List<String> getInputStream() {
		// TODO Auto-generated method stub
		return this.inputStreams;
	}

	@Override
	public String getOutputStream() {
		// TODO Auto-generated method stub
		return this.getOutputStream();
	}

	@Override
	public Map<String, IEventType> getInputSchema() {
		// TODO Auto-generated method stub
		return inputSchemas;
	}

	@Override
	public IEventType getOutputSchema() {
		// TODO Auto-generated method stub
		return outputSchema;
	}

	@Override
	public void execute(String streamName, TupleEvent event) throws StreamingException {
		firstStream.add(event);
		
	}

	@Override
	public void destroy() throws StreamingException {
		// TODO Auto-generated method stub
		firstStream.stop();
		
	}



	@Override
	public void setInputStream(List<String> streamNames) throws StreamingException {
		this.inputStreams = streamNames;
		
	}

	@Override
	public void setOutputStream(String streamName) throws StreamingException {
		this.outputStreamName = streamName;
		
	}

	@Override
	public void setInputSchema(Map<String, IEventType> schemas) throws StreamingException {
		this.inputSchemas = schemas;
		
	}

	@Override
	public void setOutputSchema(IEventType schema) throws StreamingException {
		this.outputSchema = schema;
		
	}
	

}
