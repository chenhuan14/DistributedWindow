package nudt.pdl.stormwindow.window.creator;

import java.io.Serializable;

public class WindowInfo implements Serializable{

	private static final long serialVersionUID = -2861137547017816640L;

	private String windowName;
	
	private WindowType windowType;
	
	private WindowEviction windowEviction;
	
	private boolean isSpiltEqual ;
	
	private  int slideInteval;
	/*
	 * 时间窗为秒数
	 * 数量窗为Tuple个数
	 */
	private long keepLength; 
	

	/*
	 * 默认构造函数 CountBasedTumbling（100）
	 */
	public WindowInfo()
	{
		windowType = WindowType.LengthtBased;
		windowEviction = WindowEviction.Tumbling;
		keepLength = 100;
		slideInteval = 1;
		isSpiltEqual = false;
	}
	
	

	public WindowInfo(WindowType windowType, WindowEviction windowEviction, long keepLength) {
		super();
		this.windowType = windowType;
		this.windowEviction = windowEviction;
		this.keepLength = keepLength;
	}

	public WindowInfo(WindowType windowType, WindowEviction windowEviction, long keepLength , boolean isSpiltEqual) {
		super();
		this.windowType = windowType;
		this.windowEviction = windowEviction;
		this.keepLength = keepLength;
		this.isSpiltEqual = isSpiltEqual;
	}
	
	


	/**
	 * @return the isSpiltEqual
	 */
	public boolean isSpiltEqual() {
		return isSpiltEqual;
	}



	/**
	 * @param isSpiltEqual the isSpiltEqual to set
	 */
	public void setSpiltEqual(boolean isSpiltEqual) {
		this.isSpiltEqual = isSpiltEqual;
	}



	public int getSlideInteval() {
		return slideInteval;
	}


	public void setSlideInteval(int slideInteval) {
		this.slideInteval = slideInteval;
	}


	public String getWindowName() {
		return windowName;
	}


	public void setWindowName(String windowName) {
		this.windowName = windowName;
	}


	public WindowType getWindowType() {
		return windowType;
	}


	public void setWindowType(WindowType windowType) {
		this.windowType = windowType;
	}


	public WindowEviction getWindowEviction() {
		return windowEviction;
	}


	public void setWindowEviction(WindowEviction windowEviction) {
		this.windowEviction = windowEviction;
	}


	public long getKeepLength() {
		return keepLength;
	}


	public void setKeepLength(long keepLength) {
		this.keepLength = keepLength;
	}
	
	
}
