package nudt.pdl.stormwindow.view;


import java.io.Serializable;
import java.util.List;

/**
 * 
 * <IViewable接口标记视图支持一个或者多个子视图>
 * 
 */
public interface IViewable extends Serializable
{
    /**
     * <增加子视图>
     * @param view 子视图
     * @return 子视图
     */
    public IView addView(IView view);
    
    /**
     * <获取子视图>
     * @return 子视图列表
     */
    public List<IView> getViews();
    
    /**
     * <删除子视图>
     * @param view 子视图
     * @return ture 成功 ， false 失败
     */
    public boolean removeView(IView view);
    
    /**
     * <删除全部子视图>
     */
    public void removeAllViews();
    
    /**
     * <是否包含子视图>
     * @return ture 包含子视图， false 不包含子视图
     */
    public boolean hasViews();
    
    /**
     * <启动>
     */
    public void start();
    
    /**
     * <停止>
     */
    public void stop();
}
