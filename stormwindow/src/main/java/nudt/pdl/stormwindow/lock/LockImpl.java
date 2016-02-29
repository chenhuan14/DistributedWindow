package nudt.pdl.stormwindow.lock;


import java.io.Serializable;

/**
 * 
 * 锁的实现
 * 
 */
public class LockImpl implements ILock, Serializable
{
    private static final long serialVersionUID = -226664943615880273L;
    
    private FIFOMutex lock = new FIFOMutex();
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void lock()
    {
        lock.lock();
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void unlock()
    {
        lock.unlock();
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLocked()
    {
        return lock.isLocked();
    }
    
}
