package nudt.pdl.stormwindow.view;



import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;
import nudt.pdl.stormwindow.operator.IProcessor;


/**
 * <特殊视图，使用逻辑处理器对新数据和过期数据进行逻辑处理。>
 * 
 */
public class ProcessView implements IView
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 4259489275132557001L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(ProcessView.class);
    
    /**
     * 父视图
     */
    private IViewable parent;
    
    /**
     * 处理器对象
     */
    private IProcessor processor = null;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void update(Tuple[] newData, Tuple[] oldData)
    {
        if (null == processor)
        {
            String msg = "Processor is NULL.";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
        
        processor.process(newData, oldData);
    }
    
    /**
     * <获取逻辑处理器>
     * @return 逻辑处理器
     */
    public IProcessor getProcessor()
    {
        return processor;
    }
    
    /**
     * <设置逻辑处理器>
     * @param processor 逻辑处理器
     */
    public void setProcessor(IProcessor processor)
    {
        this.processor = processor;
    }
    
    /**
     * {@inheritDoc}
     * 
     */
    @Override
    public IView addView(IView view)
    {
        throw new RuntimeException("not supported");
    }
    
    /**
     * {@inheritDoc}
     * 
     */
    @Override
    public List<IView> getViews()
    {
        throw new RuntimeException("not supported");
    }
    
    /**
     * {@inheritDoc}
     * 
     */
    @Override
    public boolean removeView(IView view)
    {
        throw new RuntimeException("not supported");
    }
    
    /**
     * {@inheritDoc}
     * 
     */
    @Override
    public void removeAllViews()
    {
        throw new RuntimeException("not supported");
    }
    
    /**
     * {@inheritDoc}
     * 
     */
    @Override
    public boolean hasViews()
    {
        throw new RuntimeException("not supported");
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IViewable getParent()
    {
        return parent;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setParent(IViewable parent)
    {
        this.parent = parent;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
        
    }
}
