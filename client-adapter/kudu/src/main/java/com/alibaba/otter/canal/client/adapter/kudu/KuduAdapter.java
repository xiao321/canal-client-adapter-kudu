package com.alibaba.otter.canal.client.adapter.kudu;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.kudu.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.kudu.config.MappingConfigLoader;
import com.alibaba.otter.canal.client.adapter.kudu.monitor.KuduConfigMonitor;
import com.alibaba.otter.canal.client.adapter.kudu.service.KuduEtlService;
import com.alibaba.otter.canal.client.adapter.kudu.service.KuduSyncService;
import com.alibaba.otter.canal.client.adapter.kudu.support.KuduTemplate;
import com.alibaba.otter.canal.client.adapter.support.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduTable;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @Title: KuduAdapter
 * @ProjectName: canal
 * @Description: TODO
 * @author: xiaobingxian
 * @date: 2018/12/18 14:00
 */
@Slf4j
@SPI("kudu")
public class KuduAdapter implements OuterAdapter {

    private Map<String, MappingConfig> kuduMapping = new HashMap<>();                          // 文件名对应配置
    private Map<String, Map<String, MappingConfig>> mappingConfigCache = new HashMap<>();

    private KuduConfigMonitor kuduConfigMonitor;
    private KuduTemplate kuduTemplate;
    private KuduSyncService kuduSyncService;

    @Override
    public void init(OuterAdapterConfig configuration) {
        Map<String, MappingConfig> kuduMappingTmp = MappingConfigLoader.load();
        // 过滤不匹配的key的配置
        kuduMappingTmp.forEach((key, mappingConfig) -> {
            if (configuration.getKey() == null) {
                kuduMapping.put(key, mappingConfig);
            }
        });
        for (Map.Entry<String, MappingConfig> entry : kuduMapping.entrySet()) {
            String configName = entry.getKey();
            MappingConfig mappingConfig = entry.getValue();
            Map<String, MappingConfig> configMap = mappingConfigCache
                    .computeIfAbsent(StringUtils.trimToEmpty(mappingConfig.getDestination()) + "."
                                    + mappingConfig.getKuduMapping().getDatabase() + "."
                                    + mappingConfig.getKuduMapping().getTable(),
                            k1 -> new HashMap<>());
            configMap.put(configName, mappingConfig);
        }
        Map<String, String> properties = configuration.getProperties();

        kuduTemplate = new KuduTemplate(properties.get("cluster"));
        kuduSyncService = new KuduSyncService(kuduTemplate);

        kuduConfigMonitor = new KuduConfigMonitor();
        kuduConfigMonitor.init(this);

    }

    @Override
    public void sync(List<Dml> dmls) {
        for (Dml dml : dmls) {
            sync(dml);
        }
    }

    public void sync(Dml dml) {
        if (dml == null) {
            return;
        }
        String destination = StringUtils.trimToEmpty(dml.getDestination());
        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, MappingConfig> configMap = mappingConfigCache.get(destination + "." + database + "." + table);
        configMap.values().forEach(config -> kuduSyncService.sync(config, dml));
    }

    @Override
    public void destroy() {
        if (kuduConfigMonitor != null) {
            kuduConfigMonitor.destroy();
        }
    }

    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult etlResult = new EtlResult();
        MappingConfig config = kuduMapping.get(task);
        if (config != null) {
            DataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
            if (dataSource != null) {
                return KuduEtlService.importData(dataSource, kuduTemplate, config, params);
            } else {
                etlResult.setSucceeded(false);
                etlResult.setErrorMessage("DataSource not found");
                return etlResult;
            }
        } else {
            StringBuilder resultMsg = new StringBuilder();
            boolean resSucc = true;
            // ds不为空说明传入的是datasourceKey
            for (MappingConfig configTmp : kuduMapping.values()) {
                // 取所有的destination为task的配置
                if (configTmp.getDestination().equals(task)) {
                    DataSource dataSource = DatasourceConfig.DATA_SOURCES.get(configTmp.getDataSourceKey());
                    if (dataSource == null) {
                        continue;
                    }
                    EtlResult etlRes = KuduEtlService.importData(dataSource, kuduTemplate, configTmp, params);
                    if (!etlRes.getSucceeded()) {
                        resSucc = false;
                        resultMsg.append(etlRes.getErrorMessage()).append("\n");
                    } else {
                        resultMsg.append(etlRes.getResultMessage()).append("\n");
                    }
                }
            }
            if (resultMsg.length() > 0) {
                etlResult.setSucceeded(resSucc);
                if (resSucc) {
                    etlResult.setResultMessage(resultMsg.toString());
                } else {
                    etlResult.setErrorMessage(resultMsg.toString());
                }
                return etlResult;
            }
        }
        etlResult.setSucceeded(false);
        etlResult.setErrorMessage("Task not found");
        return etlResult;
    }

    @Override
    public Map<String, Object> count(String task) {
        return null;
    }

    @Override
    public String getDestination(String task) {

        MappingConfig config = kuduMapping.get(task);
        if (config != null && config.getKuduMapping() != null) {
            return config.getDestination();
        }
        return null;
    }

    public Map<String, MappingConfig> getKuduMapping() {
        return kuduMapping;
    }

    public Map<String, Map<String, MappingConfig>> getMappingConfigCache() {
        return mappingConfigCache;
    }
}
