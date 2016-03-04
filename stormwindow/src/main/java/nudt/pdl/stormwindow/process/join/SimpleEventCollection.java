package nudt.pdl.stormwindow.process.join;



import java.util.LinkedHashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;




/**
 * 
 * 没有索引的待JOIN窗口有效事件保存
 * <功能详细描述>
 * 
 */
public class SimpleEventCollection implements IEventCollection
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 656105706993754245L;
    
    private static final Logger LOG = LoggerFactory.getLogger(SimpleEventCollection.class);
    
    private String streamName;
    
    private Set<Tuple> keyBasedEvent;

    
    /**
     * <默认构造函数>
     *@param name 保存的数据流名称
     *@param type 事件类型
     */
    public SimpleEventCollection(String name)
    {
        LOG.debug("Init SimpleEventCollection(). stream name={}.", name);
        this.streamName = name;

        keyBasedEvent = new LinkedHashSet<Tuple>();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void addRemove(Tuple[] newData, Tuple[] oldData)
    {
        add(newData);
        remove(oldData);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void add(Tuple[] events)
    {
        if (events == null)
        {
            return;
        }
        for (Tuple e : events)
        {
            keyBasedEvent.add(e);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void remove(Tuple[] events)
    {
        if (events == null)
        {
            return;
        }
        for (Tuple e : events)
        {
            keyBasedEvent.remove(e);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty()
    {
        return keyBasedEvent.isEmpty();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void clear()
    {
        keyBasedEvent.clear();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String getStreamName()
    {
        return this.streamName;
    }
    
    /**
     * 获得所有保存的事件（窗口有效事件）
     * <功能详细描述>
     * @return 所有保存的事件
     */
    public Set<Tuple> lookupAll()
    {
        return keyBasedEvent;
    }

    


	@Override
	public Set<Tuple> getAllEvents() {
		// TODO Auto-generated method stub
		return this.keyBasedEvent;
	}
    
}
