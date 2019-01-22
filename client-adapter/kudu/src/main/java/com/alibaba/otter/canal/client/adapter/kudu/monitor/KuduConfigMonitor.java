package com.alibaba.otter.canal.client.adapter.kudu.monitor;

import com.alibaba.otter.canal.client.adapter.kudu.KuduAdapter;
import com.alibaba.otter.canal.client.adapter.kudu.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import com.alibaba.otter.canal.client.adapter.support.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.commons.lang.StringUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * @Title: KuduConfigMonitor
 * @ProjectName: canal
 * @Description: TODO
 * @author: xiaobingxian
 * @date: 2018/12/24 19:43
 */
@Slf4j
public class KuduConfigMonitor {

    private static final String adapterName = "kudu";
    private KuduAdapter kuduAdapter;

    private FileAlterationMonitor fileMonitor;

    public void init(KuduAdapter kuduAdapter) {
        this.kuduAdapter = kuduAdapter;
        File confDir = Util.getConfDirPath(adapterName);
        try {
            FileAlterationObserver observer = new FileAlterationObserver(confDir,
                    FileFilterUtils.and(FileFilterUtils.fileFileFilter(), FileFilterUtils.suffixFileFilter("yml")));
            FileListener listener = new FileListener();
            observer.addListener(listener);
            fileMonitor = new FileAlterationMonitor(3000, observer);
            fileMonitor.start();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void destroy() {
        try {
            fileMonitor.stop();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private class FileListener extends FileAlterationListenerAdaptor {

        @Override
        public void onFileCreate(File file) {
            super.onFileCreate(file);
            try {
                // 加载新增的配置文件
                String configContent = MappingConfigsLoader.loadConfig(adapterName + File.separator + file.getName());
                MappingConfig config = new Yaml().loadAs(configContent, MappingConfig.class);
                config.validate();
                addConfigToCache(file, config);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileChange(File file) {
            super.onFileChange(file);

            try {
                if (kuduAdapter.getKuduMapping().containsKey(file.getName())) {
                    // 加载配置文件
                    String configContent = MappingConfigsLoader.loadConfig(adapterName + File.separator + file.getName());
                    MappingConfig config = new Yaml().loadAs(configContent, MappingConfig.class);
                    config.validate();
                    if (kuduAdapter.getKuduMapping().containsKey(file.getName())) {
                        deleteConfigFromCache(file);
                    }
                    addConfigToCache(file, config);
                    log.info("Change a kudu mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileDelete(File file) {
            super.onFileDelete(file);

            try {
                if (kuduAdapter.getKuduMapping().containsKey(file.getName())) {
                    deleteConfigFromCache(file);

                    log.info("Delete a kudu mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        private void addConfigToCache(File file, MappingConfig config) {
            kuduAdapter.getKuduMapping().put(file.getName(), config);
            Map<String, MappingConfig> configMap = kuduAdapter.getMappingConfigCache()
                    .computeIfAbsent(StringUtils.trimToEmpty(config.getDestination()) + "."
                                    + config.getKuduMapping().getDatabase() + "." + config.getKuduMapping().getTable(),
                            k1 -> new HashMap<>());
            configMap.put(file.getName(), config);
        }

        private void deleteConfigFromCache(File file) {

            kuduAdapter.getKuduMapping().remove(file.getName());
            for (Map<String, MappingConfig> configMap : kuduAdapter.getMappingConfigCache().values()) {
                if (configMap != null) {
                    configMap.remove(file.getName());
                }
            }

        }
    }
}
