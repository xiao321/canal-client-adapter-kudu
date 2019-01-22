package com.alibaba.otter.canal.client.adapter.kudu.config;

import com.alibaba.otter.canal.client.adapter.kudu.support.KuduColumnItem;
import lombok.Data;
import org.apache.kudu.Type;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * @Title: MappingConfig
 * @ProjectName: canal
 * @Description: TODO
 * @author: xiaobingxian
 * @date: 2018/12/22 16:07
 */
@Data
public class MappingConfig {
    private String    dataSourceKey;   // 数据源key
    private String    destination;     // canal实例或MQ的topic
    private KuduMapping kuduMapping;       // db映射配置


    public void validate() {
        if (kuduMapping.database == null || kuduMapping.database.isEmpty()) {
            throw new NullPointerException("kuduMapping.database");
        }
        if (kuduMapping.table == null || kuduMapping.table.isEmpty()) {
            throw new NullPointerException("kuduMapping.table");
        }
        if (kuduMapping.targetTable == null || kuduMapping.targetTable.isEmpty()) {
            throw new NullPointerException("kuduMapping.targetTable");
        }
        if (kuduMapping.targetKey == null || kuduMapping.targetKey.isEmpty()) {
            throw new NullPointerException("kuduMapping.targetKey");
        }
    }

    @Data
    public static class KuduMapping {

        private String database; // 数据库名或schema名
        private String table; // 表面名
        private String targetTable; // 目标表名
        private String etlCondition;// etl条件sql
        private String targetCluster;
        private Map<String, String> targetKey; // 目标表主键字段
        private Map<String, String> targetColumns; // 目标表字段映射
        // 配置文件中的字段映射转换后的映射列表
        private Map<String, KuduColumnItem> columnItems = new HashMap<>();
        private Map<String, KuduColumnItem> rowkey = new HashMap<>();

        public void setTargetKeys(Map<String, String> targetKey) {
            this.targetKey = targetKey;

            if(targetKey != null) {
                targetKey.forEach((k,v) -> {
                    String[] strs = v.split("$");
                    KuduColumnItem columnItem = new KuduColumnItem();
                    columnItem.setColumnName(strs[0]);
                    columnItem.setColumnType(Type.valueOf(strs[1].toLowerCase()));
                    this.rowkey.put(k, columnItem);
                });

            }
        }

        public void setTargetColumns(Map<String, String> targetColumns) {
            this.targetColumns = targetColumns;

            if(targetColumns != null) {
                targetColumns.forEach((k,v) -> {
                    String[] strs = v.split("$");
                    KuduColumnItem columnItem = new KuduColumnItem();
                    columnItem.setColumnName(strs[0]);
                    columnItem.setColumnType(Type.valueOf(strs[1].toLowerCase()));
                    this.columnItems.put(k, columnItem);
                });
            }
        }
    }

}
