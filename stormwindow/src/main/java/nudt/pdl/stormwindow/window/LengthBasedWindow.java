package nudt.pdl.stormwindow.window;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nudt.pdl.stormwindow.view.IDataCollection;
import nudt.pdl.stormwindow.view.IRenew;
import nudt.pdl.stormwindow.view.ViewImpl;

/**
 * <长度窗口抽象类>
 * 
 */
public abstract class LengthBasedWindow extends ViewImpl implements IWindow, IRenew
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -591855531989393141L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(LengthBasedWindow.class);
    
    /**
     * 窗口事件保持长度
     */
    private long keepLength;
    
    /**
     * 窗口事件缓存集合
     */
    private IDataCollection dataCollection;
    
    /**
     * <默认构造函数>
     *@param keepLength 窗口保持长度
     */
    public LengthBasedWindow(long keepLength)
    {
        super();
        if (keepLength > 0)
        {
            this.keepLength = keepLength;
            LOG.debug("Length Window Keep Length: {}.", keepLength);
        }
        else
        {
            LOG.error("Invalid keepLength:  {}.", keepLength);
            throw new IllegalArgumentException("Invalid keepLength: " + keepLength);
        }
    }
    
    /**
     * <获取窗口保持长度 >
     * @return 窗口保持长度
     */
    public long getKeepLength()
    {
        return keepLength;
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
