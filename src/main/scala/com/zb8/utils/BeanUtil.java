package com.zb8.utils;

import com.alibaba.fastjson.JSON;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class BeanUtil {
    private static Logger logger = LoggerFactory.getLogger(BeanUtil.class);

    /**
     * @param obj
     * @return
     */
    public static Map<String, Object> bean2Map(Object obj) {
        if (obj == null) return null;
        Map map = null;
        try {
            map = BeanUtils.describe(obj);
            map.remove("class");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return map;
    }

    /**
     * Map转换成Bean，使用泛型免去了类型转换的麻烦(大小写敏感)。
     *
     * @param <T>
     * @param map
     * @param class1
     * @return
     */
    public static <T> T map2Bean(Map<String, Object> map, Class<T> class1) {
        T bean = null;
        try {
            bean = class1.newInstance();
            BeanUtils.populate(bean, map);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return bean;
    }

    /**
     * Map转换成Bean，使用泛型免去了类型转换的麻烦(忽略大小写)。
     *
     * @param <T>
     * @param map
     * @param clazz
     * @return
     */
    public static <T> T map2BeanIgnoreCase(Map<String, Object> map, Class<T> clazz) {
        return JSON.parseObject(JSON.toJSONString(map), clazz);
    }

    /**
     * 获取实体的所有可见属性值
     *
     * @param object 实体类的实例
     * @return 返回一个Object数组，该数组包含实体的所有可见属性值
     */
    public static Object[] getFieldValuesAsArray(Object object) {
        if (object == null) return null;
        Field[] fields = object.getClass().getDeclaredFields();
        List<Object> fieldValueList = new ArrayList<>();
        try {
            for (Field f : fields) {
                //修饰符代码：PUBLIC: 1，PRIVATE: 2，PROTECTED: 4，STATIC: 8，FINAL: 16等
                if (f.getModifiers() > 2) continue;
                Object property1 = PropertyUtils.getProperty(object, f.getName());
                fieldValueList.add(property1);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return fieldValueList.toArray();
    }

    /**
     * 获取实体的所有可见属性值
     *
     * @param object 实体类的实例
     * @return 返回一个TreeMap，该数组包含实体的所有可见属性值
     */
    public static Map getFieldValuesAsLinkMap(Object object) {
        if (object == null) return null;
        Field[] fields = object.getClass().getDeclaredFields();
        Map<String, Object> fieldValueMap = new LinkedHashMap<>();
        try {
            for (Field f : fields) {
                //修饰符代码：PUBLIC: 1，PRIVATE: 2，PROTECTED: 4，STATIC: 8，FINAL: 16等
                if (f.getModifiers() > 2) continue;
                fieldValueMap.put(f.getName(), BeanUtils.getProperty(object, f.getName()));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return fieldValueMap;
    }
}
