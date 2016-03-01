package nudt.pdl.stormwindow.view;


import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Tuple;
import nudt.pdl.stormwindow.expression.IExpression;


/**
 * 
 * 过滤功能视图
 * <功能详细描述>
 * 
 */
public class FilterView extends ViewImpl implements IRenew
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -6936987505013321181L;
    
    private IExpression boolexpr;
    
    /**
     * <默认构造函数>
     *@param exp 过滤BOOLEAN表达式
     */
    public FilterView(IExpression exp)
    {
        this.boolexpr = exp;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void update(Tuple[] newData, Tuple[] oldData)
    {
        Tuple[] newE = filterEvent(newData, true);
        Tuple[] oldE = filterEvent(oldData, false);
        
        if (newE != null || oldE != null)
        {
            this.updateChild(newE, oldE);
        }
    }
    
    private Tuple[] filterEvent(Tuple[] events, boolean isNewData)
    {
        if (events == null)
        {
            return null;
        }
        
        List<Tuple> qualified = new ArrayList<Tuple>();
        Boolean pass = null;
        for (Tuple e : events)
        {
            pass = (Boolean)boolexpr.evaluate(e);
            if (null != pass && pass)
            {
                qualified.add(e);
            }
        }
        
        if (qualified.size() > 0)
        {
            return qualified.toArray(new Tuple[qualified.size()]);
        }
        else
        {
            return null;
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IView renewView()
    {
        return new FilterView(boolexpr);
    }
    
    public IExpression getBoolexpr()
    {
        return boolexpr;
    }
}
