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
    
    
	private String outputStream;
    
    private List<String> inputStreams;
    
    /**
     * <默认构造函数>
     */
    public AbsOperator()
    {
    	inputStreams = new ArrayList<String>();
    	inputStreams.add(Constant.DEFAULT_INPUT_STREAM);
		outputStream = Constant.DEFAULT_OUTPUT_STREAM;
    	
    }
    

    
    @Override
	public List<String> getInputStream() {
		// TODO Auto-generated method stub
		return this.inputStreams;
	}

	@Override
	public String getOutputStream() {
		// TODO Auto-generated method stub
		return this.outputStream;
	}

	
	
	public void setInputStream(List<String> streamNames)  {
		this.inputStreams = streamNames;
		
	}

	
	public void setOutputStream(String streamName)  {
		this.outputStream = streamName;
		
	}




    /**
     * 添加输入流
     * @param streamName 输入流名称
     */
    public void addInputStream(String streamName)
    {
        if (!StringUtils.isEmpty(streamName))
        {
            if (!inputStreams.contains(streamName))
            {
                inputStreams.add(streamName);
            }
        }
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