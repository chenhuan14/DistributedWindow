package nudt.pdl.stormwindow.window;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nudt.pdl.stormwindow.lock.ILock;
import nudt.pdl.stormwindow.lock.LockImpl;
import nudt.pdl.stormwindow.timer.ITimerCallBack;
import nudt.pdl.stormwindow.timer.TimeService;
import nudt.pdl.stormwindow.view.IDataCollection;
import nudt.pdl.stormwindow.view.ViewImpl;


/**
 * <时间窗口抽象类>
 * 
 */
public abstract class TimeBasedWindow extends ViewImpl implements IWindow, ITimerCallBack, ILock
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -1920607211633345740L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(TimeBasedWindow.class);
    
    /**
     * 窗口事件保持时间
     */
    private long keepTime;
    
    /**
     * 定时器服务
     */
    private TimeService timeservice = null;
    
    /**
     * 锁对象，当前线程与定时器线程访问数据时需要先获取锁。
     */
    private ILock lock;
    
    /**
     * 窗口事件缓存集合
     */
    private IDataCollection dataCollection;
    
    /**
     * <默认构造函数>
     *@param keepTime 窗口保持时间
     */
    public TimeBasedWindow(long keepTime)
    {
        super();
        if (keepTime > 0)
        {
            this.keepTime = keepTime;
            LOG.debug("Time window KeepTime: {}.", keepTime);
        }
        else
        {
            LOG.error("Invalid keepTime: {}.", keepTime);
            throw new IllegalArgumentException("Invalid keepTime: " + keepTime);
        }
    }
    
    /**
     * <获取时间窗口保持时间>
     * @return 窗口保持时间
     */
    protected long getKeepTime()
    {
        return keepTime;
    }
    
    /**
     * <获取定时器服务>
     * @return 定时器服务对象
     */
    public TimeService getTimeservice()
    {
        return timeservice;
    }
    
    /**
     * <设置定时器服务>
     * @param timeservice 定时器服务
     */
    public void setTimeservice(TimeService timeservice)
    {
        this.timeservice = timeservice;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void lock()
    {
        lock.lock();
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void unlock()
    {
        lock.unlock();
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLocked()
    {
        return lock.isLocked();
    }
    
    /**
     * 初始化锁对象
     * 
     */
    public void initLock()
    {
        if (lock == null)
        {
            lock =  new LockImpl();
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        initLock();
        
        if (timeservice != null)
        {
            timeservice.startInternalClock();
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
        if (timeservice != null)
        {
            timeservice.stopInternalClock();
        }
    }
    
    /**
     * {@inheritDoc}
     */
    public void setDataCollection(IDataCollection dataCollection)
    {
        if (dataCollection == null)
        {
            LOG.error("Invalid dataCollection.");
            throw new IllegalArgumentException("Invalid dataCollection");
        }
        
        this.dataCollection = dataCollection;
    }
    
    /**
     * 返回窗口事件缓存集
     * @return 窗口事件缓存集
     */
    public IDataCollection getDataCollection()
    {
        return dataCollection;
    }
}
