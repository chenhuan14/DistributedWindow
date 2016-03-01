package nudt.pdl.stormwindow.operator;

import nudt.pdl.stormwindow.event.TupleEvent;
import nudt.pdl.stormwindow.exception.StreamingException;

import nudt.pdl.stormwindow.view.FirstLevelStream;
import nudt.pdl.stormwindow.view.ProcessView;
import nudt.pdl.stormwindow.window.IWindow;
import nudt.pdl.stormwindow.window.creator.WindowCreator;
import nudt.pdl.stormwindow.window.creator.WindowInfo;

public abstract class AbsWindowedOperator extends AbsOperator implements IProcessor {

	private static final long serialVersionUID = -7032576167789745669L;
	private WindowInfo windowInfo ;
	private FirstLevelStream firstStream ;
	private IWindow window;
	


  
	public AbsWindowedOperator()
	{
		windowInfo = new WindowInfo();
		firstStream = new FirstLevelStream();
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
	public void execute(String streamName, TupleEvent event) throws StreamingException {
		firstStream.add(event);
	}

	@Override
	public void destroy() throws StreamingException {
		// TODO Auto-generated method stub
		firstStream.stop();
		
	}

	

}
