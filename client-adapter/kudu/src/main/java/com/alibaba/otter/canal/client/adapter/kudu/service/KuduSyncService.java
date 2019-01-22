package com.alibaba.otter.canal.client.adapter.kudu.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.adapter.kudu.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.kudu.support.KuduColumnItem;
import com.alibaba.otter.canal.client.adapter.kudu.support.KuduTemplate;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Title: KuduSyncService
 * @ProjectName: canal
 * @Description: TODO
 * @author: xiaobingxian
 * @date: 2018/12/25 19:50
 */
@Slf4j
public class KuduSyncService {

    private KuduTemplate kuduTemplate; // HBase操作模板

    public KuduSyncService(KuduTemplate kuduTemplate){
        this.kuduTemplate = kuduTemplate;
    }

    public void sync(MappingConfig config, Dml dml) {
        try {
            if (config != null) {
                String type = dml.getType();
                if (type == null || !type.equalsIgnoreCase("INSERT")) {
                    if (type != null && type.equalsIgnoreCase("UPDATE")) {
                        update(config, dml);
                    } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                        delete(config, dml);
                    }
                } else {
                    insert(config, dml);
                }

                log.debug("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void insert(MappingConfig config, Dml dml) {
        List<Map<String, Object>> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        MappingConfig.KuduMapping kuduMapping = config.getKuduMapping();
        // 插入数据
        for (Map<String, Object> r : data) {
            List<KuduColumnItem> row = new ArrayList<>();
            r.forEach((k,v) -> {
                KuduColumnItem columnItem = new KuduColumnItem();
                columnItem.setColumnName(kuduMapping.getColumnItems().get(k).getColumnName());
                columnItem.setColumnValue(v.toString());
                row.add(columnItem);
            });

            kuduTemplate.insert(kuduMapping.getTargetTable(), row);
        }
    }

    private void update(MappingConfig config, Dml dml) {
        List<Map<String, Object>> data = dml.getData();
        List<Map<String, Object>> old = dml.getOld();
        if (old == null || old.isEmpty() || data == null || data.isEmpty()) {
            return;
        }

        MappingConfig.KuduMapping kuduMapping = config.getKuduMapping();
        //更新数据
        for (Map<String, Object> r : data) {
            List<KuduColumnItem> row = new ArrayList<>();

            r.forEach((k,v) -> {
                KuduColumnItem columnItem = new KuduColumnItem();
                columnItem.setColumnName(kuduMapping.getColumnItems().get(k).getColumnName());
                columnItem.setColumnValue(v.toString());
                row.add(columnItem);
            });
            kuduTemplate.update(kuduMapping.getTargetTable(), row);
        }
    }

    private void delete(MappingConfig config, Dml dml) {
        List<Map<String, Object>> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        MappingConfig.KuduMapping kuduMapping = config.getKuduMapping();
        // 暂不支持复合key
        Map<String, KuduColumnItem> key = kuduMapping.getRowkey();
        String sourceKey = (String) key.keySet().toArray()[0];
        KuduColumnItem targetKey = key.get(sourceKey);

        for (Map<String, Object> d : data) {
            if(d.containsKey(sourceKey)) {
                targetKey.setColumnValue(d.get(sourceKey).toString());
                kuduTemplate.delete(kuduMapping.getTargetTable(), targetKey);
            }
        }

    }
}
