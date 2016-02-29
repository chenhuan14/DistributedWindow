package nudt.pdl.stormwindow.window.creator;


import nudt.pdl.stormwindow.window.IWindow;
import nudt.pdl.stormwindow.window.KeepAllWindow;
import nudt.pdl.stormwindow.window.LengthBatchWindow;
import nudt.pdl.stormwindow.window.LengthSlideWindow;
import nudt.pdl.stormwindow.window.TimeBatchWindow;
import nudt.pdl.stormwindow.window.TimeSlideWindow;

public class  WindowCreator {
	public static IWindow createInstance(WindowInfo info)
	{
		WindowType type = info.getWindowType();
		WindowEviction eviction = info.getWindowEviction();
		if(type == WindowType.KeepAll)
			return new KeepAllWindow();
		else if(type == WindowType.LengthtBased && eviction == WindowEviction.Sliding)
			return new LengthSlideWindow(info.getKeepLength());
		else if((type == WindowType.LengthtBased && eviction == WindowEviction.Tumbling))
			return new LengthBatchWindow(info.getKeepLength());
		else if((type == WindowType.TimeBased && eviction == WindowEviction.Sliding))
			return new TimeSlideWindow(info.getKeepLength(), info.getSlideInteval());
		else if ((type == WindowType.TimeBased && eviction == WindowEviction.Tumbling))
			return new TimeBatchWindow(info.getKeepLength());
		else 
			return null;
	}
}
