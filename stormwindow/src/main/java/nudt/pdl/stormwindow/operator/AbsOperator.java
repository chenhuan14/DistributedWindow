package nudt.pdl.stormwindow.operator;

import java.util.ArrayList;

import java.util.List;
import org.apache.commons.lang.StringUtils;
import nudt.pdl.stormwindow.util.Constant;


/**
 * 基础的流处理算子实现类
 * 只实现了基本的并发设置，参数设置等方法
 * <p/>
 * Streaming内部实现都依赖于此类
 * 外部Storm相关不感知此类
 *
 */
public abstract class AbsOperator implements IRichOperator
{
    
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 9152942480213961636L;
    
    private int parallelNumber;
    
    private String operatorId;
    
    

    
    /**
     * <默认构造函数>
     */
    public AbsOperator()
    {
    	
    	
    }
    


    

    public String getOperatorId()
    {
        return this.operatorId;
    }
    
    /**
     * 设置算子id
     *
     * @param id 算子id
     */
    public void setOperatorId(String id)
    {
        operatorId = id;
    }

    public int getParallelNumber()
    {
        return parallelNumber;
    }
    
    /**
     * 设置算子并发度
     *
     * @param number 并发度
     */
    public void setParallelNumber(int number)
    {
        this.parallelNumber = number;
    }
    
   
  
   
    
}