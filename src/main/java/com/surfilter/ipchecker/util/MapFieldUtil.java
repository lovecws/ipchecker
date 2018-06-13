package com.surfilter.ipchecker.util;

import java.util.Map;

public class MapFieldUtil {

    /**
     * 根据字段的名称获取字段的值
     *
     * @param fieldName 字段名称 可以获取嵌套字段
     * @return
     */
    @SuppressWarnings("rawtypes")
    public static String getFieldValue(Map _sourceMap, String fieldName) {
        if (fieldName == null || "".equals(fieldName)) {
            return null;
        }
        String[] fieldNames = fieldName.split("\\.");
        Map map = _sourceMap;
        for (int i = 0; i < fieldNames.length; i++) {
            Object object = map.get(fieldNames[i]);
            if (i == fieldNames.length - 1) {
                return object != null ? object.toString() : null;
            } else {
                map = (Map) object;
            }
        }
        return null;
    }

    /**
     * 获取统计字段
     *
     * @param eventMap
     * @param fieldName
     * @return
     */
    public static String getStatisticalField(Map eventMap, String fieldName) {
        String eventType = eventMap.get("eventType").toString();
        String temFieldName = fieldName;
        if ("worm".equalsIgnoreCase(eventType)) {
            temFieldName = fieldName.replace("ip_operator", "ip_operator");
        } else if ("bot_or_trojan".equals(eventType)) {
            temFieldName = fieldName.replace("ip_operator", "src_ip_operator");
        } else if ("bot_or_trojan_c".equals(eventType)) {
            temFieldName = fieldName.replace("ip_operator", "dst_ip_operator");
        }
        String statisticalField = null;
        for (String fd : temFieldName.split(",")) {
            String fieldValue = getFieldValue(eventMap, fd);
            if (statisticalField == null) {
                statisticalField = fieldValue;
            } else {
                statisticalField = statisticalField + "-" + fieldValue;
            }
        }
        return statisticalField;
    }
}
