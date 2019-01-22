package com.alibaba.otter.canal.client.adapter.kudu.config;

import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Title: ConfigLoader
 * @ProjectName: canal
 * @Description: TODO
 * @author: xiaobingxian
 * @date: 2018/12/22 16:06
 */
@Slf4j
public class MappingConfigLoader {

    /**
     * 加载kudu配置文件
     * @return
     */
    public static Map<String, MappingConfig> load() {
        log.info("## Start loading kudu mapping config ... ");

        Map<String, MappingConfig> result = new LinkedHashMap<>();

        Map<String, String> configContentMap = MappingConfigsLoader.loadConfigs("kudu");
        configContentMap.forEach((fileName, content) -> {
            MappingConfig config = new Yaml().loadAs(content, MappingConfig.class);
            try {
                config.validate();
            } catch (Exception e) {
                throw new RuntimeException("ERROR Config: " + fileName + " " + e.getMessage(), e);
            }
            result.put(fileName, config);
        });

        log.info("## kudu mapping config loaded");
        return result;
    }
}
