package nudt.pdl.stormwindow.operator;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import backtype.storm.tuple.Tuple;
import nudt.pdl.stormwindow.exception.StreamingException;
import nudt.pdl.stormwindow.util.Constant;
import nudt.pdl.stormwindow.view.FirstLevelStream;
import nudt.pdl.stormwindow.view.ProcessView;
import nudt.pdl.stormwindow.window.IWindow;
import nudt.pdl.stormwindow.window.creator.WindowCreator;
import nudt.pdl.stormwindow.window.creator.WindowInfo;



/**
 * 单窗口处理抽象类
 * @author Administrator
 *
 */

public abstract class AbsWindowedOperator extends AbsOperator implements IProcessor {

	private static final long serialVersionUID = -7032576167789745669L;
	private WindowInfo windowInfo ;
	private FirstLevelStream firstStream ;
	private IWindow window;
	
	private String outputStream;  
    private List<String> inputStreams;

	public AbsWindowedOperator()
	{
		super();
		windowInfo = new WindowInfo();
		firstStream = new FirstLevelStream();
		
		inputStreams = new ArrayList<String>();
    	inputStreams.add(Constant.DEFAULT_INPUT_STREAM);
		outputStream = Constant.DEFAULT_OUTPUT_STREAM;
	}
    
	public void setWindowInfo(WindowInfo info)
	{
		this.windowInfo = info;
	}
	
	public WindowInfo getWindowInfo()
	{
		return this.windowInfo;
	}

	
	/*
	 * 每一个Operator默认都有一个名为default的输入流和一个名为default的输入流
	 */
	@Override
	public void initialize() throws StreamingException {
		
		ProcessView processview = new ProcessView();
		processview.setProcessor(this);
		
		window = WindowCreator.createInstance(windowInfo);
		if(null == window)
		{
			throw new StreamingException("no such window type");
		}
		else
		{
			window.addView(processview);
			firstStream.addView(window);
			firstStream.start();
		}
	}
	

	

	@Override
	public void execute(String streamName, Tuple event) throws StreamingException {
		firstStream.add(event);
	}

	@Override
	public void destroy() throws StreamingException {
		// TODO Auto-generated method stub
		firstStream.stop();
		
	}

	
    
    @Override
	public List<String> getInputStream() {
		// TODO Auto-generated method stub
		return this.inputStreams;
	}

	@Override
	public String getOutputStream() {
		// TODO Auto-generated method stub
		return this.outputStream;
	}

	
	
	public void setInputStream(List<String> streamNames)  {
		this.inputStreams = streamNames;
		
	}

	
	public void setOutputStream(String streamName)  {
		this.outputStream = streamName;
		
	}




    /**
     * 添加输入流
     * @param streamName 输入流名称
     */
    public void addInputStream(String streamName)
    {
        if (!StringUtils.isEmpty(streamName))
        {
            if (!inputStreams.contains(streamName))
            {
                inputStreams.add(streamName);
            }
        }
    }
    
	

}
