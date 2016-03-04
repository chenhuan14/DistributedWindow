package nudt.pdl.stormwindow.view;


import java.util.LinkedHashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <特殊视图，调用Join处理器完成Join操作>
 * 
 */
public class JoinProcessView extends ProcessView
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 3503873571996480982L;
    
    private static final Logger LOG = LoggerFactory.getLogger(JoinProcessView.class);
    
    private Set<IView> parents;
    
    /**
     * <默认构造函数>
     *
     */
    public JoinProcessView()
    {
        LOG.debug("Initiate JoinProcessView.");
        parents = new LinkedHashSet<IView>();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IViewable getParent()
    {
        throw new RuntimeException("getParent() is not supported in JoinProcessView");
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setParent(IViewable parent)
    {
        LOG.debug("set {} as parent view to JoinProcessView.", parent);
        parents.add((IView)parent);
    }
    
    /**
     * 获得所有父视图
     * <功能详细描述>
     * @return 返回所有父视图
     */
    public IViewable[] getParents()
    {
        return parents.toArray(new IView[parents.size()]);
    }
}
