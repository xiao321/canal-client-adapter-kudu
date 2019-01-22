package com.alibaba.otter.canal.client.adapter.kudu.service;

import com.alibaba.otter.canal.client.adapter.kudu.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.kudu.support.KuduColumnItem;
import com.alibaba.otter.canal.client.adapter.kudu.support.KuduTemplate;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.JdbcTypeUtil;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Title: KuduEtlService
 * @ProjectName: canal
 * @Description: TODO
 * @author: xiaobingxian
 * @date: 2018/12/25 19:50
 */
@Slf4j
public class KuduEtlService {

    /**
     * 创建 kudu 表
     * @param kuduTemplate
     * @param config
     */
    public static void createTable(KuduTemplate kuduTemplate, MappingConfig config) {
        try {
            // 判断hbase表是否存在，不存在则建表
            MappingConfig.KuduMapping kuduMapping = config.getKuduMapping();
            if (!kuduTemplate.tableExists(kuduMapping.getTargetTable())) {
                kuduTemplate.createTable(kuduMapping.getTargetTable(), kuduMapping.getRowkey(), kuduMapping.getColumnItems());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public static EtlResult importData(DataSource ds, KuduTemplate kuduTemplate, MappingConfig config,
                                       List<String> params) {
        EtlResult etlResult = new EtlResult();
        AtomicLong successCount = new AtomicLong();
        List<String> errMsg = new ArrayList<>();
        String kuduTable = "";
        try {
            if (config == null) {
                log.error("Config is null!");
                etlResult.setSucceeded(false);
                etlResult.setErrorMessage("Config is null!");
                return etlResult;
            }
            MappingConfig.KuduMapping kuduMapping = config.getKuduMapping();
            kuduTable = kuduMapping.getTargetTable();

            long start = System.currentTimeMillis();

            if (params != null && params.size() == 1 && "rebuild".equalsIgnoreCase(params.get(0))) {
                log.info(kuduMapping.getTargetTable() + " rebuild is starting!");
                // 如果表存在则删除
                if (kuduTemplate.tableExists(kuduMapping.getTargetTable())) {
                    kuduTemplate.deleteTable(kuduMapping.getTargetTable());
                }
                params = null;
            } else {
                log.info(kuduMapping.getTargetTable() + " etl is starting!");
            }
            createTable(kuduTemplate, config);

            // 拼接mysql sql
            String sql = "SELECT * FROM " + kuduMapping.getDatabase() + "." + kuduMapping.getTable();

            // 拼接条件
            if (params != null && params.size() == 1 && kuduMapping.getEtlCondition() == null) {
                AtomicBoolean stExists = new AtomicBoolean(false);
                // 验证是否有SYS_TIME字段
                Util.sqlRS(ds, sql, rs -> {
                    try {
                        ResultSetMetaData rsmd = rs.getMetaData();
                        int cnt = rsmd.getColumnCount();
                        for (int i = 1; i <= cnt; i++) {
                            String columnName = rsmd.getColumnName(i);
                            if ("SYS_TIME".equalsIgnoreCase(columnName)) {
                                stExists.set(true);
                                break;
                            }
                        }
                    } catch (Exception e) {
                        // ignore
                    }
                    return null;
                });
                if (stExists.get()) {
                    sql += " WHERE SYS_TIME >= '" + params.get(0) + "' ";
                }
            } else if (kuduMapping.getEtlCondition() != null && params != null) {
                String etlCondition = kuduMapping.getEtlCondition();
                int size = params.size();
                for (int i = 0; i < size; i++) {
                    etlCondition = etlCondition.replace("{" + i + "}", params.get(i));
                }

                sql += " " + etlCondition;
            }

            // 获取总数
            String countSql = "SELECT COUNT(1) FROM ( " + sql + ") _CNT ";
            long cnt = (Long) Util.sqlRS(ds, countSql, rs -> {
                Long count = null;
                try {
                    if (rs.next()) {
                        count = ((Number) rs.getObject(1)).longValue();
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
                return count == null ? 0 : count;
            });

            executeSqlImport(ds, sql, kuduMapping, kuduTemplate, successCount, errMsg);

            log.info(kuduMapping.getTargetTable() + " etl completed in: "
                    + (System.currentTimeMillis() - start) / 1000 + "s!");

            etlResult.setResultMessage("导入kudu表 " + kuduMapping.getTargetTable() + " 数据：" + successCount.get() + " 条");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            errMsg.add(kuduTable + " etl failed! ==>" + e.getMessage());
        }

        if (errMsg.isEmpty()) {
            etlResult.setSucceeded(true);
        } else {
            etlResult.setErrorMessage(Joiner.on("\n").join(errMsg));
        }
        return etlResult;
    }

    private static boolean executeSqlImport(DataSource ds, String sql, MappingConfig.KuduMapping kuduMapping,
                                            KuduTemplate kuduTemplate, AtomicLong successCount, List<String> errMsg) {
        try {
            Util.sqlRS(ds, sql, rs -> {
                int i = 1;
                try {
                    while (rs.next()) {
                        int cc = rs.getMetaData().getColumnCount();
                        int[] jdbcTypes = new int[cc];
                        for (int j = 1; j <= cc; j++) {
                            int jdbcType = rs.getMetaData().getColumnType(j);
                            jdbcTypes[j - 1] = jdbcType;
                        }
                        // 获取 字段 值
                        List<KuduColumnItem> columns = new ArrayList<>();
                        for (int j = 1; j <= cc; j++) {
                            String columnName = rs.getMetaData().getColumnName(j);

                            Object val = JdbcTypeUtil.getRSData(rs, columnName, jdbcTypes[j - 1]);
                            if (val == null) {
                                continue;
                            }

                            KuduColumnItem columnItem = new KuduColumnItem();
                            columnItem.setColumnName(kuduMapping.getColumnItems().get(columnName).getColumnName());
                            columnItem.setColumnValue(val.toString());
                            columns.add(columnItem);
                        }

                        // 单行插入
                        kuduTemplate.insert(kuduMapping.getTargetTable(), columns);
                        i++; //计数
                        successCount.incrementAndGet();

                        log.debug("successful import count:" + successCount.get());
                    }

                } catch (Exception e) {
                    log.error(kuduMapping.getTargetTable() + " etl failed! ==>" + e.getMessage(), e);
                    errMsg.add(kuduMapping.getTargetTable() + " etl failed! ==>" + e.getMessage());
                    // throw new RuntimeException(e);
                }
                return i;
            });
            return true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

}
