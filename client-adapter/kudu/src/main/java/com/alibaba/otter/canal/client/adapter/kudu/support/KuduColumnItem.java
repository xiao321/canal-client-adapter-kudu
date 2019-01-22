package com.alibaba.otter.canal.client.adapter.kudu.support;

import lombok.Data;
import org.apache.kudu.Type;

/**
 * @Title: KuduColumnItem
 * @ProjectName: canal
 * @Description: kudu 字段
 * @author: xiaobingxian
 * @date: 2019/1/21 19:54
 */
@Data
public class KuduColumnItem {
    private String columnName;
    private Type columnType;
    private String columnValue;
}
