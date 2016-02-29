package nudt.pdl.stormwindow.event;



import java.io.Serializable;

/**
 * 
 * 事件类型接口
 * 
 */
public interface IEventType extends Serializable
{
    /**
     * 获取某个属性的类型
     * @param attName 属性名称
     * @return 属性类型，返回结果Java类型
     */
    public Attribute getAttribute(String attName);
    
    /**
     * 获得所有属性类型
     * <功能详细描述>
     * @return 所有属性类型
     */
    public Attribute[] getAllAttributes();
    
    /**
     * 获取属性名称类表
     * @return 属性名称列表
     */
    public String[] getAllAttributeNames();
    
    /**
     * 获取属性类型
     * @return 属性名称列表
     */
    public Class< ? >[] getAllAttributeTypes();
    
    /**
     * 事件类型名称
     * @return 名称
     */
    public String getEventTypeName();
    
    /**
     * 
     * 获取属性名称的编号
     * @param propertyName 属性名称
     * @return 编号
     */
    public int getAttributeIndex(String propertyName);
    
    /**
     * 获取指定编号的属性名称
     * @param index 编号
     * @return 属性名称
     */
    public String getAttributeName(int index);
    
    /**
     * 获得事件类型中属性个数
     * <功能详细描述>
     * @return 属性个数
     */
    public int getSize();
    
}
