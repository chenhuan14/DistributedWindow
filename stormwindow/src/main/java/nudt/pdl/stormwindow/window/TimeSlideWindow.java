package nudt.pdl.stormwindow.window;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;
import nudt.pdl.stormwindow.timer.TimeService;
import nudt.pdl.stormwindow.view.IDataCollection;



/**
 * <时间滑动窗口实现类>
 * 
 */
public class TimeSlideWindow extends TimeBasedWindow
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 1444510944409525421L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(TimeSlideWindow.class);
    
    /**
     * 窗口事件保存对象，记录时间与事件的索引方便得到过期事件
     */
    private TimeSlideEventList events = new TimeSlideEventList();
    
    /**
     * <默认构造函数>
     *@param keepTime 窗口保持时间，单位：ms
     *@param slideInterval 窗口滑动间隔，单位：ms
     */
    public TimeSlideWindow(long keepTime, long slideInterval)
    {
        super(keepTime);
        
        TimeService timeservice = new TimeService(slideInterval, this);
        setTimeservice(timeservice);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void update(Tuple[] newData, Tuple[] oldData)
    {
        //TODO 未考虑窗口叠加时，上一个窗口传递的过期数据的处理。
        
        if ((null == newData) || (0 == newData.length))
        {
            LOG.error("Input Time Slide Window newData is Null!");
            return;
        }
        
        try
        {
            lock();
            //TODO 存在时间漂移的问题，会导致数据顺序不准确。
            long timestamp = System.currentTimeMillis();
            //TODO 是否需要考虑窗口中的事件过期，当前处理认为事件过期已经通过定时处理，来新事件时，不存在过期数据。
            for (int i = 0; i < newData.length; i++)
            {
                events.add(timestamp + getKeepTime(), newData[i]);
            }
            
            IDataCollection dataCollection = getDataCollection();
            if (dataCollection != null)
            {
                dataCollection.update(newData, null);
            }
            
            if (this.hasViews())
            {
                updateChild(newData, null);
            }
        }
        finally
        {
            if (isLocked())
            {
                unlock();
            }
        }
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void timerCallBack(long currentTime)
    {
        try
        {
            lock();
            Tuple[] oldData = events.getOldData(currentTime);
            
            if (null == oldData)
            {
                return;
            }
            else
            {
                IDataCollection dataCollection = getDataCollection();
                if (dataCollection != null)
                {
                    dataCollection.update(null, oldData);
                }
                
                if (this.hasViews())
                {
                    updateChild(null, oldData);
                }
            }
        }
        finally
        {
            if (isLocked())
            {
                unlock();
            }
        }
    }
    
}
