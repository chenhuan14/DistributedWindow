package nudt.pdl.stormwindow.timer;



import java.util.concurrent.ScheduledFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <定时器任务，根据定时参数调用回调对象处理方法>
 * 
 */
public class TimerTask implements Runnable
{
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(TimerTask.class);
    
    /**
     * 定时器回调对象
     */
    private final ITimerCallBack callback;
    
    /**
     * 定时器
     */
    @SuppressWarnings("unused")
    private ScheduledFuture< ? > future = null;
    
    /**
     * <默认构造函数>
     *@param callback 定时器回调对象
     */
    public TimerTask(ITimerCallBack callback)
    {
        this.callback = callback;
    }
    
    /**
     * {@inheritDoc}
     */
    public final void run()
    {
        try
        {
            long currentTime = System.currentTimeMillis();
            callback.timerCallBack(currentTime);
        }
        catch (Throwable t)
        {
            LOG.error("Timer thread caught unhandled exception: " + t.getMessage(), t);
        }
    }
    
    /**
     * <设置运行结果>
     * @param future 运行结果
     */
    public void setFuture(ScheduledFuture< ? > future)
    {
        this.future = future;
    }
    
}
