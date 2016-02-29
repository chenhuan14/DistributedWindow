package nudt.pdl.stormwindow.common;


import java.io.Serializable;
import java.util.Arrays;

/**
 * <多值类>
 * 
 */
public class MultiKey implements Serializable
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 1753414850747666008L;
    
    private static final int HASH_BASE_CODE = 31;
    
    private final Object[] keys;
    
    private final int hashCode;
    
    /**
     * <默认构造函数>
     *@param keys 值数组
     */
    public MultiKey(Object[] keys)
    {
        if (keys == null)
        {
            throw new IllegalArgumentException("The array of keys must not be null.");
        }
        
        int total = 0;
        for (int i = 0; i < keys.length; i++)
        {
            if (keys[i] != null)
            {
                total *= HASH_BASE_CODE;
                total ^= keys[i].hashCode();
            }
        }
        
        this.hashCode = total;
        this.keys = keys;
    }
    
    /**
     * <返回大小>
     * @return 大小
     */
    public final int size()
    {
        return keys.length;
    }
    
    /**
     * <返回索引对应值>
     * @param index 索引
     * @return 值
     */
    public final Object get(int index)
    {
        return keys[index];
    }
    
    /**
     * {@inheritDoc}
     */
    public final boolean equals(Object other)
    {
        if (other == this)
        {
            return true;
        }
        if (other instanceof MultiKey)
        {
            MultiKey otherKeys = (MultiKey)other;
            return Arrays.equals(keys, otherKeys.keys);
        }
        return false;
    }
    
    /**
     * <返回数据>
     * @return 数据
     */
    public Object[] getKeys()
    {
        return keys;
    }
    
    /**
     * {@inheritDoc}
     */
    public final int hashCode()
    {
        return hashCode;
    }
    
    /**
     * {@inheritDoc}
     */
    public final String toString()
    {
        return "MultiKey" + Arrays.asList(keys).toString();
    }
}
