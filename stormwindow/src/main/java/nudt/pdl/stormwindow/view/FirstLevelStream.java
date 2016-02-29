package nudt.pdl.stormwindow.view;


import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nudt.pdl.stormwindow.event.IEvent;


/**
 * 
 * <FirstLevelStream 实现第一层视图处理>
 * 
 */
public final class FirstLevelStream implements IViewable
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -4993118574495471408L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(FirstLevelStream.class);
    
    /**
     * 子视图集合
     */
    private final LinkedList<IView> children = new LinkedList<IView>();
    
    /**
     * 最后加入事件
     */
    private IEvent lastInsertedEvent;
    
    /**
     * <接受外部数据>
     * @param theEvent 新事件
     */
    public final void add(IEvent theEvent)
    {
        IEvent[] newData = new IEvent[] {theEvent};
        for (IView childView : children)
        {
            childView.update(newData, null);
        }
        
        lastInsertedEvent = theEvent;
    }
    
    /**
     * {@inheritDoc}
     */
    public final IView addView(IView view)
    {
        if (null == view)
        {
            String msg = "View is NULL.";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
        
        children.add(view);
        view.setParent(this);
        return view;
    }
    
    /**
     * {@inheritDoc}
     */
    public final List<IView> getViews()
    {
        return children;
    }
    
    /**
     * {@inheritDoc}
     */
    public final boolean removeView(IView view)
    {
        if (null == view)
        {
            String msg = "View is NULL.";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
        
        boolean isRemoved = children.remove(view);
        view.setParent(null);
        return isRemoved;
    }
    
    /**
     * {@inheritDoc}
     */
    public final boolean hasViews()
    {
        return !children.isEmpty();
    }
    
    /**
     * {@inheritDoc}
     */
    public void removeAllViews()
    {
        children.clear();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        for (IView childView : children)
        {
            childView.start();
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
        for (IView childView : children)
        {
            childView.stop();
        }
    }
    
    /**
     * 返回 lastInsertedEvent
     * @return 返回 lastInsertedEvent
     */
    public final IEvent getLastInsertedEvent()
    {
        return lastInsertedEvent;
    }
    
}
