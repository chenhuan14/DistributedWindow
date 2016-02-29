package nudt.pdl.stormwindow.output;



import java.io.Serializable;

/**
 * 数据输出接口
 * <功能详细描述>
 * 
 */
public interface IOutput extends Serializable
{
    /**
     * 输出处理
     * <功能详细描述>
     * @param object 输出对象
     */
    public void output(Object object);
}
