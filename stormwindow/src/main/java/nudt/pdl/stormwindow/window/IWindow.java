package nudt.pdl.stormwindow.window;

import nudt.pdl.stormwindow.view.IDataCollection;
import nudt.pdl.stormwindow.view.IView;

/**
 * <Window接口，Window允许保存上一个View发送来的数据。Window 可以为长度窗口，也可以为时间窗口。>
 * 
 */
public interface IWindow extends IView
{
    /**
     * <设置窗口数据缓存集>
     * <设置窗口数据缓存集，用于后续表达式访问窗口数据>
     * @param dataCollection 数据缓存集
     */
    public void setDataCollection(IDataCollection dataCollection);
}
