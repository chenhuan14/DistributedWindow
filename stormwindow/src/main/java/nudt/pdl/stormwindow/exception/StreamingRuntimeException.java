package nudt.pdl.stormwindow.exception;



/**
 * Streaming运行时异常
 * 所有的运行时异常，都继承自该类，或者直接抛出该类
 * 
 */
public class StreamingRuntimeException extends RuntimeException
{
    
    /**
     * 序列化
     */
    private static final long serialVersionUID = 1120103984629512199L;




    /** <默认构造函数>
     *
     */
    public StreamingRuntimeException()
    {
    }
    
    /** <默认构造函数>
     *@param message 异常消息
     */
    public StreamingRuntimeException(String message)
    {
        super(message);
    }
    
    /** <默认构造函数>
     *@param cause 异常堆栈
     */
    public StreamingRuntimeException(Throwable cause)
    {
        super(cause);
    }
    
    /** <默认构造函数>
     *@param message 异常消息
     *@param cause 异常堆栈
     */
    public StreamingRuntimeException(String message, Throwable cause)
    {
        super(message, cause);
    }
    
    /** <默认构造函数>
     *@param message 异常消息
     *@param cause 异常堆栈
     *@param enableSuppression whether or not suppression is enabled
     *                          or disabled
     *@param writableStackTrace whether or not the stack trace should
     *                           be writable
     */
    public StreamingRuntimeException(String message, Throwable cause, boolean enableSuppression,
        boolean writableStackTrace)
    {
        super(message, cause, enableSuppression, writableStackTrace);
    }


   

}
