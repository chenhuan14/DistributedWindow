package nudt.pdl.stormwindow.view;

import backtype.storm.tuple.Tuple;

/**
 * Functor功能视图
 * 是否需要判断左值和右值的合法性
 */
public class FunctorView extends ViewImpl
{
    
    private static final long serialVersionUID = -7149861251125803604L;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void update(Tuple[] newData, Tuple[] oldData)
    {
        updateChild(newData, oldData);
    }
    
}
