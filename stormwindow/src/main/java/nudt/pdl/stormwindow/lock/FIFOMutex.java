package nudt.pdl.stormwindow.lock;


import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * 
 * 先进先出的互斥锁
 * 
 */
public class FIFOMutex implements Serializable
{
    private static final long serialVersionUID = 5517911381008004130L;
    
    private final AtomicBoolean locked = new AtomicBoolean(false);
    
    private final transient Queue<Thread> waiters = new ConcurrentLinkedQueue<Thread>();
    
    /**
     * < 锁定>
     */
    public void lock()
    {
        boolean wasInterrupted = false;
        Thread current = Thread.currentThread();
        waiters.add(current);
        
        // Block while not first in queue or cannot acquire lock
        while (waiters.peek() != current || !locked.compareAndSet(false, true))
        {
            LockSupport.park(this);
            if (Thread.interrupted())
            {
                wasInterrupted = true;
            }
        }
        
        waiters.remove();
        if (wasInterrupted)
        {
            current.interrupt();
        }
    }
    
    /**
     * <取消锁定>
     */
    public void unlock()
    {
        locked.set(false);
        LockSupport.unpark(waiters.peek());
    }
    
    /**
     * <是否被锁定>
     * @return 是否被锁定
     */
    public boolean isLocked()
    {
        return locked.get();
    }
}
