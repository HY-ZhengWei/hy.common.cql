package org.hy.common.xcql;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.hy.common.Date;
import org.hy.common.Help;
import org.hy.common.MethodReflect;
import org.hy.common.StaticReflect;
import org.hy.common.StringHelp;
import org.neo4j.driver.types.MapAccessorWithDefaultValue;





/**
 * 解释 fill 或 cfill 字符串后生成的方法信息
 * 
 * 这样只须解释一次，在后面的行级填充动作时，可快速填充，而不用每次都解释 fill 或 cfill 字符串。
 * 
 * @author      ZhengWei(HY)
 * @createDate  2023-06-03
 * @version     v1.0
 */
public final class XCQLMethod
{
    /** 调用的方法 */
    private Method                               call;
    
    /** 调用方法的入参信息 */
    private List<XCQLMethodParam>                paramList;
    
    /**
     * 当 XSQLResult.cfill 等于 "setter(colValue)" 时（即，XSQLResult.$CFILL_METHOD_VARY），此值才会有效。
     * 
     * 其意为按 call 方法的入参类型，决定 org.neo4j.driver.Result 获取字段值的方法。
     * 而不是用 if 语句在填充数据时才判断，而是预先解释好，填充数据时直接调用相关实例化的类，来提高性能。
     * 
     *    1. 当 call 的入参类型为 int.class    时，此值为 Result.getInt   (int i_ColNo) 方法
     *    2. 当 call 的入参类型为 String.class 时，此值为 Result.getString(int i_ColNo) 方法
     *    3. ... 等等
     * 
     * 这样做的最终目的是：提高性能
     */
    private Method                               resultSet_Getter;
    
    /**
     * 加工 Result.getXXX(int i_ColNo) 返回值。
     * 
     * 如：Result.getTimestamp() 返回值，要 Setter 到入参类型为 java.util.Date 的方法中，就需要加工。
     * 
     * 与 resultSet_Getter 搭配着使用
     * 
     * 这样做的最终目的是：再一次提高性能
     */
    private MachiningValue                       machiningValue;
    
    /**
     * 相对于 this.call 而言，获取 this.call 方法的实例对象的 Getter 方法对象。
     * 
     * 用于：对象A的属性还是一个对象B，现对对象B的属性进行填充。
     *      this.getInstanceOfMethod 的值为对象A中 "get对象B()" 的方法
     * 
     * ZhengWei(HY) Add 2015-07-04
     */
    private Method                               getInstanceOfMethod;
    
    /**
     * 与 this.getInstanceOfMethod 类似。主要作用为：当对象B为空时，实例化对象B
     * 此属性为 "set对象B(...)" 的方法
     * 
     * ZhengWei(HY) Add 2015-07-04
     */
    private Method                               setInstanceOfMethod;
    
    /**
     * 一对多关系时的 "多对象" 的类型。它是个集合对象。
     * 当此属性有值时，this.getInstanceOfMethod 属性即为 "多对象" 的Getter方法
     * 
     * ZhengWei(HY) Add 2017-03-02
     */
    private Class<?>                             collection;
    
    /**
     * 一对多关系时的 "多对象" 的集合元素的类型。
     * 当此属性有值时，this.getInstanceOfMethod 属性即为 "多对象" 的Getter方法
     * 
     * ZhengWei(HY) Add 2017-03-02
     */
    private Class<?>                             collectionElement;
    
    
    
    public XCQLMethod()
    {
        this.call                = null;
        this.paramList           = new ArrayList<XCQLMethodParam>();
        this.resultSet_Getter    = null;
        this.machiningValue      = null;
        this.getInstanceOfMethod = null;
        this.setInstanceOfMethod = null;
        this.collection          = null;
        this.collectionElement   = null;
    }
    
    
    
    /**
     * 执行后，得到将"子级"对象填充到"父级"对象中的父级填充方法的参数值
     * 如：行级对象填充到表级对象
     * 如：列级数值填充表行级对象
     * 
     * @param i_Father     父级对象。     当i_Child为行级对象时，为表级对象
     *                                  当i_Child为列级字段值时，为行级对象
     * @param i_Child      填充对象。     可以行级对象 或 列级字段值
     * @param i_ChildNo    填充对象的编号。当为行级对象时，为行号。  下标从 0 开始。
     *                                  当为列级字段值时，为空 null，Neo4j每个节点的属性可以不一致，没有固定的结构，
     *                                   也就无从列号之说了哈
     * @param i_ChildName  填充对象的名称。当为行级对象时，可为空 null
     *                                  当为列级字段值时，为字段名称
     */
    @SuppressWarnings("unchecked")
    public void invoke(Object i_Father ,Object i_Child ,Long i_ChildNo ,String i_ChildName)
    {
        Object [] v_Values = new Object[this.paramList.size()];
        
        for (int i=0; i<this.paramList.size(); i++)
        {
            v_Values[i] = this.paramList.get(i).invoke(i_Child ,i_ChildNo ,i_ChildName);
        }
        
        try
        {
            // 对象A的属性还是一个对象B，现对对象B的属性进行填充。ZhengWei(HY) Add 2015-07-04
            if ( this.getInstanceOfMethod != null )
            {
                Object  v_FatherTemp            = this.getInstanceOfMethod.invoke(i_Father);
                Object  v_CollectionElementTemp = null;
                boolean v_ValuesIsNull          = Help.isNullByAll(v_Values);
                
                
                if ( v_FatherTemp == null )
                {
                    if ( this.setInstanceOfMethod == null )
                    {
                        // 没有对应的 "set对象B(...)"的方法。此时就不再执行填充动作
                        return;
                    }
                    
                    try
                    {
                        // 支持对象B为集合的情况。A与B为：一对多关系。 ZhengWei(HY) Add 2017-03-01
                        if ( this.collection == List.class )
                        {
                            v_FatherTemp = new ArrayList<Object>();
                            
                            if ( !v_ValuesIsNull )
                            {
                                v_CollectionElementTemp = this.collectionElement.newInstance();
                                ((List<Object>)v_FatherTemp).add(v_CollectionElementTemp);
                            }
                        }
                        else if ( this.collection == Set.class )
                        {
                            v_FatherTemp = new LinkedHashSet<Object>();
                            
                            if ( !v_ValuesIsNull )
                            {
                                v_CollectionElementTemp = this.collectionElement.newInstance();
                                ((Set<Object>)v_FatherTemp).add(v_CollectionElementTemp);
                            }
                        }
                        else
                        {
                            v_FatherTemp = this.getInstanceOfMethod.getReturnType().newInstance();
                        }
                    }
                    catch (Exception exce)
                    {
                        // 当对象B实例化失败时，就不再执行填充动作
                        return;
                    }
                    
                    this.setInstanceOfMethod.invoke(i_Father ,v_FatherTemp);
                }
                
                
                // 支持对象B为集合的情况。A与B为：一对多关系。 ZhengWei(HY) Add 2017-03-01
                if ( this.collection == List.class )
                {
                    if ( v_CollectionElementTemp == null )
                    {
                        List<Object> v_CollectionTemp = (List<Object>)v_FatherTemp;
                        
                        if ( Help.isNull(v_CollectionTemp) )
                        {
                            if ( !v_ValuesIsNull )
                            {
                                v_CollectionElementTemp = this.collectionElement.newInstance();
                                v_CollectionTemp.add(v_CollectionElementTemp);
                            }
                        }
                        else
                        {
                            v_CollectionElementTemp = v_CollectionTemp.get(v_CollectionTemp.size() - 1);
                        }
                    }
                    
                    if ( v_CollectionElementTemp != null )
                    {
                        this.call.invoke(v_CollectionElementTemp ,v_Values);
                    }
                }
                else if ( this.collection == Set.class )
                {
                    if ( v_CollectionElementTemp == null )
                    {
                        Set<Object> v_CollectionTemp = (Set<Object>)v_FatherTemp;
                        
                        if ( Help.isNull(v_CollectionTemp) )
                        {
                            if ( !v_ValuesIsNull )
                            {
                                v_CollectionElementTemp = this.collectionElement.newInstance();
                                v_CollectionTemp.add(v_CollectionElementTemp);
                            }
                        }
                        else
                        {
                            v_CollectionElementTemp = v_CollectionTemp.iterator().next();
                        }
                    }
                    
                    if ( v_CollectionElementTemp != null )
                    {
                        this.call.invoke(v_CollectionElementTemp ,v_Values);
                    }
                }
                else
                {
                    this.call.invoke(v_FatherTemp ,v_Values);
                }
            }
            else
            {
                this.call.invoke(i_Father ,v_Values);
            }
        }
        catch (Exception exce)
        {
            String v_VString = "";
            if ( !Help.isNull(v_Values) )
            {
                v_VString = StringHelp.toString(v_Values);
            }
            throw new RuntimeException(i_Father.getClass().getName() + "." + this.call.getName() + "(" + v_VString + ").\n" + exce.getMessage());
        }
    }
    
    
    
    /**
     * 添加解释后的方法的参数信息
     * 
     * @param i_MethodParam
     */
    public void addParam(XCQLMethodParam i_XCQLMethodParam)
    {
        if ( i_XCQLMethodParam == null )
        {
            throw new NullPointerException("Method add parameter is null.");
        }
        
        this.paramList.add(i_XCQLMethodParam);
    }
    
    
    
    /**
     * 按 call 方法的入参类型，决定 org.neo4j.driver.Result 获取字段值的方法
     * 
     * 当 XCQLResult.cfill 等于 "setter(colValue)" 时（即，XCQLResult.$CFILL_METHOD_VARY），此值才会有效。
     */
    @SuppressWarnings("unchecked")
    public void parseResultSet_Getter()
    {
        if ( this.call == null )
        {
            return;
        }
        
        
        Class<?> v_SetterParamClass = this.call.getParameterTypes()[0];
        
        try
        {
            this.machiningValue = new MachiningDefault();
            
            if ( v_SetterParamClass == String.class )
            {
                this.resultSet_Getter = MapAccessorWithDefaultValue.class.getDeclaredMethod("get" ,String.class ,String.class);
            }
            else if ( v_SetterParamClass == int.class
                   || v_SetterParamClass == Integer.class )
            {
                this.resultSet_Getter = MapAccessorWithDefaultValue.class.getDeclaredMethod("get" ,String.class ,int.class);
            }
            else if ( v_SetterParamClass == BigDecimal.class )
            {
                this.resultSet_Getter = MapAccessorWithDefaultValue.class.getDeclaredMethod("get" ,String.class ,double.class);
            }
            else if ( v_SetterParamClass == double.class
                   || v_SetterParamClass == Double.class )
            {
                this.resultSet_Getter = MapAccessorWithDefaultValue.class.getDeclaredMethod("get" ,String.class ,double.class);
            }
            else if ( v_SetterParamClass == Date.class )
            {
                this.resultSet_Getter = MapAccessorWithDefaultValue.class.getDeclaredMethod("get" ,String.class ,String.class);
                this.machiningValue   = new MachiningMyDate();
            }
            else if ( v_SetterParamClass == java.util.Date.class )
            {
                this.resultSet_Getter = MapAccessorWithDefaultValue.class.getDeclaredMethod("get" ,String.class ,String.class);
                this.machiningValue   = new MachiningDate();
            }
            else if ( v_SetterParamClass == boolean.class
                   || v_SetterParamClass == Boolean.class )
            {
                this.resultSet_Getter = MapAccessorWithDefaultValue.class.getDeclaredMethod("get" ,String.class ,boolean.class);
            }
            else if ( v_SetterParamClass == long.class
                   || v_SetterParamClass == Long.class )
            {
                this.resultSet_Getter = MapAccessorWithDefaultValue.class.getDeclaredMethod("get" ,String.class ,long.class);
            }
            else if ( v_SetterParamClass == short.class
                   || v_SetterParamClass == Short.class )
            {
                this.resultSet_Getter = MapAccessorWithDefaultValue.class.getDeclaredMethod("get" ,String.class ,int.class);
            }
            else if ( v_SetterParamClass == byte.class
                   || v_SetterParamClass == Byte.class )
            {
                this.resultSet_Getter = MapAccessorWithDefaultValue.class.getDeclaredMethod("get" ,String.class ,int.class);
            }
            else if ( v_SetterParamClass == byte[].class
                   || v_SetterParamClass == Byte[].class )
            {
                 this.resultSet_Getter = MapAccessorWithDefaultValue.class.getDeclaredMethod("get" ,String.class ,String.class);
            }
            else if ( MethodReflect.isExtendImplement(v_SetterParamClass ,Enum.class) )
            {
                // 从原来的getInt方法改为getString方法，支持数字、字符类型的常量类  ZhengWei(HY) Edit 2018-05-11
                this.resultSet_Getter = MapAccessorWithDefaultValue.class.getDeclaredMethod("get" ,String.class ,int.class);
                this.machiningValue   = new MachiningEnum((Class<? extends Enum<?>>)v_SetterParamClass);
            }
            else
            {
                this.resultSet_Getter = MapAccessorWithDefaultValue.class.getDeclaredMethod("get" ,String.class ,String.class);
            }
        }
        catch (Exception exce)
        {
            throw new RuntimeException(exce.getMessage());
        }
    }
    
    
    
    public Method getCall()
    {
        return this.call;
    }
    
    
    
    public void setCall(Method i_Call)
    {
        this.call = i_Call;
    }
    
    
    
    public Method getResultSet_Getter()
    {
        return resultSet_Getter;
    }
    
    
    
    public void setResultSet_Getter(Method resultSet_Getter)
    {
        this.resultSet_Getter = resultSet_Getter;
    }
    
    
    
    public MachiningValue getMachiningValue()
    {
        return machiningValue;
    }
    
    
    
    /**
     * 获取：相对于 this.call 而言，获取 this.call 方法的实例对象的 Getter 方法对象。
     * 
     * 用于：对象A的属性还是一个对象B，现对对象B的属性进行填充。
     *      this.getInstanceOfMethod 的值为对象A中 "get对象B()" 的方法
     * 
     * ZhengWei(HY) Add 2015-07-04
     */
    public Method getGetInstanceOfMethod()
    {
        return getInstanceOfMethod;
    }


    
    /**
     * 设置：相对于 this.call 而言，获取 this.call 方法的实例对象的 Getter 方法对象。
     * 
     * 用于：对象A的属性还是一个对象B，现对对象B的属性进行填充。
     *      this.getInstanceOfMethod 的值为对象A中 "get对象B()" 的方法
     * 
     * ZhengWei(HY) Add 2015-07-04
     * 
     * @param getInstanceOfMethod
     */
    public void setGetInstanceOfMethod(Method i_GetInstanceOfMethod)
    {
        this.getInstanceOfMethod = i_GetInstanceOfMethod;
    }


    
    /**
     * 获取：与 this.getInstanceOfMethod 类似。主要作用为：当对象B为空时，实例化对象B
     * 此属性为 "set对象B(...)" 的方法
     * 
     * ZhengWei(HY) Add 2015-07-04
     */
    public Method getSetInstanceOfMethod()
    {
        return setInstanceOfMethod;
    }


    
    /**
     * 设置：与 this.getInstanceOfMethod 类似。主要作用为：当对象B为空时，实例化对象B
     * 此属性为 "set对象B(...)" 的方法
     * 
     * ZhengWei(HY) Add 2015-07-04
     * 
     * @param setInstanceOfMethod
     */
    public void setSetInstanceOfMethod(Method setInstanceOfMethod)
    {
        this.setInstanceOfMethod = setInstanceOfMethod;
    }


    
    /**
     * 获取：一对多关系时的 "多对象" 的类型。它是个集合对象。
     * 当此属性有值时，this.getInstanceOfMethod 属性即为 "多对象" 的Getter方法
     * 
     * ZhengWei(HY) Add 2017-03-02
     */
    public Class<?> getCollection()
    {
        return collection;
    }


    
    /**
     * 设置：一对多关系时的 "多对象" 的类型。它是个集合对象。
     * 当此属性有值时，this.getInstanceOfMethod 属性即为 "多对象" 的Getter方法
     * 
     * ZhengWei(HY) Add 2017-03-02
     * 
     * @param collection
     */
    public void setCollection(Class<?> collection)
    {
        this.collection = collection;
    }


    
    /**
     * 获取：一对多关系时的 "多对象" 的集合元素的类型。
     * 当此属性有值时，this.getInstanceOfMethod 属性即为 "多对象" 的Getter方法
     * 
     * ZhengWei(HY) Add 2017-03-02
     */
    public Class<?> getCollectionElement()
    {
        return collectionElement;
    }


    
    /**
     * 设置：一对多关系时的 "多对象" 的集合元素的类型。
     * 当此属性有值时，this.getInstanceOfMethod 属性即为 "多对象" 的Getter方法
     * 
     * ZhengWei(HY) Add 2017-03-02
     * 
     * @param collectionElement
     */
    public void setCollectionElement(Class<?> collectionElement)
    {
        this.collectionElement = collectionElement;
    }



    public void clear()
    {
        this.call                = null;
        this.resultSet_Getter    = null;
        this.getInstanceOfMethod = null;
        this.setInstanceOfMethod = null;
        this.collection          = null;
        this.collectionElement   = null;
        this.paramList.clear();
    }
    
}





/**
 * 加工接口。
 * 
 * 加工 Result.getXXX(int i_ColNo) 返回值。
 * 
 * 如：Result.getTimestamp() 返回值，要 Setter 到入参类型为 java.util.Date 的方法中，就需要加工。
 * 
 * 与 resultSet_Getter 搭配着使用
 * 
 * 这样做的最终目的是：再一次提高性能
 * 
 * 减少填充数据时 if 语句的判断，改用预先解释好，填充数据时直接调用相关实例化的类，来提高性能
 * 
 * @author      ZhengWei(HY)
 * @version     v1.0
 * @createDate  2012-11-08
 */
interface MachiningValue<R ,V>
{
    
    public R getValue(V i_Value);
    
}





/**
 * 默认的加工类
 * 
 * @author      ZhengWei(HY)
 * @version     v1.0
 * @createDate  2012-11-08
 */
class MachiningDefault implements MachiningValue<Object ,Object>
{

    @Override
    public Object getValue(Object i_Value)
    {
        return i_Value;
    }
    
}





/**
 * java.util.Date的加工类
 * 
 * @author      ZhengWei(HY)
 * @version     v1.0
 * @createDate  2012-11-08
 */
class MachiningDate implements MachiningValue<java.util.Date ,String>
{

    @Override
    public java.util.Date getValue(String i_Value)
    {
        if ( i_Value == null )
        {
            return null;
        }
        else
        {
            return new Date(i_Value).getDateObject();
        }
    }
    
}





/**
 * org.hy.common.Date的加工类
 * 
 * @author      ZhengWei(HY)
 * @version     v1.0
 * @createDate  2012-11-08
 */
class MachiningMyDate implements MachiningValue<Date ,String>
{

    @Override
    public Date getValue(String i_Value)
    {
        if ( i_Value == null )
        {
            return null;
        }
        else
        {
            return new Date(i_Value);
        }
    }
    
}





/**
 * 枚举类型的加工类
 * 
 * @author      ZhengWei(HY)
 * @createDate  2014-04-16
 * @version     v1.0
 *              v2.0  2018-05-08  添加：支持枚举名称的匹配
 */
class MachiningEnum implements MachiningValue<Enum<?> ,Object>
{
    private Enum<?> [] enums;
    
    
    public MachiningEnum(Class<? extends Enum<?>> i_EnumClass)
    {
        this.enums = StaticReflect.getEnums(i_EnumClass);
    }
    
    
    @Override
    public Enum<?> getValue(Object i_Value)
    {
        if ( i_Value == null )
        {
            return null;
        }
        else
        {
            String v_Value = i_Value.toString();
            
            // ZhengWei(HY) Add 2018-05-08  支持枚举toString()的匹配
            for (Enum<?> v_Enum : this.enums)
            {
                if ( v_Value.equalsIgnoreCase(v_Enum.toString()) )
                {
                    return v_Enum;
                }
            }
            
            // ZhengWei(HY) Add 2018-05-08  支持枚举名称的匹配
            for (Enum<?> v_Enum : this.enums)
            {
                if ( v_Value.equalsIgnoreCase(v_Enum.name()) )
                {
                    return v_Enum;
                }
            }
            
            // 尝试用枚举值匹配
            if ( Help.isNumber(v_Value) )
            {
                int v_IntValue = Integer.parseInt(v_Value.trim());
                if ( 0 <= v_IntValue && v_IntValue < this.enums.length )
                {
                    return this.enums[v_IntValue];
                }
            }
            
            return null;
        }
    }
    
}