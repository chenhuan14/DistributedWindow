package nudt.pdl.stormwindow.lock;



/**
 * 
 * 支持锁的接口
 * 
 */
public interface ILock
{
    /**
     * 锁定
     */
    void lock();
    
    /**
     * 取消锁定
     */
    void unlock();
    
    /**
     * 是否被锁定
     * @return 是否被锁定
     */
    boolean isLocked();
    
}
