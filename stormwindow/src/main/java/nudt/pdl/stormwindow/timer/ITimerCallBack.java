package nudt.pdl.stormwindow.timer;



/**
 * <定时器回调接口。窗口业务关心>
 * 
 */
public interface ITimerCallBack
{
    /**
     *  <定时器回调执行方法>
     * @param currentTime 系统当前时间
     */
    void timerCallBack(long currentTime);
}
