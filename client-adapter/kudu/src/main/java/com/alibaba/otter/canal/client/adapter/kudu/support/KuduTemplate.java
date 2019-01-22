package com.alibaba.otter.canal.client.adapter.kudu.support;

import com.alibaba.otter.canal.client.adapter.kudu.config.MappingConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Common;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Title: KuduTemplate
 * @ProjectName: canal
 * @Description: kudu 操作类
 * @author: xiaobingxian
 * @date: 2019/1/20 12:17
 */
@Slf4j
public class KuduTemplate {

    private KuduClient kuduClient;

    public KuduTemplate(String cluster) {
        KuduClient.KuduClientBuilder kuduClientBuilder = new KuduClient.KuduClientBuilder(cluster);
        kuduClientBuilder.workerCount(1);
        kuduClient = kuduClientBuilder.build();
    }

    public KuduSession getClientSession() {
        KuduSession session = kuduClient.newSession();
        //用于设定清理缓存的时间点，如果FlushMode是MANUAL或NEVEL,在操作过程中hibernate会将事务设置为readonly
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        //session.setFlushInterval(150000/2);//milliseconds
        session.setTimeoutMillis(80000);
        session.setIgnoreAllDuplicateRows(true);
        session.setMutationBufferSpace(150000);
        return session;
    }

    /**
     * 判断表是否存在
     * @param tableName
     * @return
     */
    public boolean tableExists(String tableName) {
        try {
            return kuduClient.tableExists(tableName);
        } catch (KuduException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 创建表
     * @param tableName
     */
    public void createTable(String tableName, Map<String, KuduColumnItem> keys, Map<String, KuduColumnItem> columns) {
        try {

            List<ColumnSchema> columnsList = new ArrayList<>();
            keys.forEach((k,v) -> {
                ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder(v.getColumnName(), v.getColumnType()).key(true).build();
                columnsList.add(columnSchema);
            });
            columns.forEach( (k, v) -> {
                ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder(v.getColumnName(), v.getColumnType()).build();
                columnsList.add(columnSchema);
            });
            Schema schema = new Schema(columnsList);
            if ( ! kuduClient.tableExists(tableName) ) {
                kuduClient.createTable(tableName, schema, new CreateTableOptions());
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 删除表
     * @param tableName
     */
    public void deleteTable(String tableName) {
        try {
            kuduClient.deleteTable(tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 插入数据
     * @param tableName
     * @param columns
     * @return
     */
    public Boolean insert(String tableName, List<KuduColumnItem> columns) {
        boolean flag = false;
        try {
            KuduSession kuduSession = getClientSession();
            KuduTable kuduTable = kuduClient.openTable(tableName);
            Schema schema = kuduTable.getSchema();
            Insert insert = kuduTable.newInsert();
            PartialRow partialRow = insert.getRow();
            columns.forEach(item -> {
                fillRow(partialRow, schema.getColumn(item.getColumnName()).getType(), item.getColumnName(), item.getColumnValue());
            });
            kuduSession.apply(insert);
            flag = true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return flag;
    }

    /**
     * 更新数据
     * @Description columns要包含主键
     * @param tableName
     * @param columns
     * @return
     */
    public Boolean update(String tableName, List<KuduColumnItem> columns) {
        boolean flag = false;
        try {
            KuduSession kuduSession = getClientSession();
            KuduTable kuduTable = kuduClient.openTable(tableName);
            Schema schema = kuduTable.getSchema();
            Update update = kuduTable.newUpdate();
            PartialRow partialRow = update.getRow();
            columns.forEach(item -> {
                fillRow(partialRow, schema.getColumn(item.getColumnName()).getType(), item.getColumnName(), item.getColumnValue());
            });
            kuduSession.apply(update);
            flag = true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return flag;
    }

    /**
     * 删除数据
     * @param tableName
     * @param key 主键
     * @return
     */
    public Boolean delete(String tableName, KuduColumnItem key) {
        boolean flag = false;
        try {
            KuduSession kuduSession = getClientSession();
            KuduTable kuduTable = kuduClient.openTable(tableName);
            Schema schema = kuduTable.getSchema();
            Delete delete = kuduTable.newDelete();
            PartialRow partialRow = delete.getRow();
            fillRow(partialRow, schema.getColumn(key.getColumnName()).getType(), key.getColumnName(), key.getColumnValue());

            kuduSession.apply(delete);
            flag = true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return flag;
    }

    /**
     * 填充 row
     * @param row
     * @param columnType
     * @param columnName
     * @param columnValue
     */
    private void fillRow(PartialRow row, Type columnType, String columnName, String columnValue) {
        switch (columnType) {
            case BOOL:
                row.addBoolean(columnName, Boolean.parseBoolean(columnValue));
                break;
            case FLOAT:
                row.addFloat(columnName, Float.parseFloat(columnValue));
                break;
            case DOUBLE:
                row.addDouble(columnName, Double.parseDouble(columnValue));
                break;
            case BINARY:
                row.addBinary(columnName, columnValue.getBytes());
                break;
            case INT8:
                row.addByte(columnName, Byte.parseByte(columnValue));
                break;
            case INT16:
                row.addShort(columnName, Short.parseShort(columnValue));
                break;
            case INT32:
                row.addInt(columnName, Integer.parseInt(columnValue));
                break;
            case INT64:
                row.addLong(columnName, Long.parseLong(columnValue));
                break;
            case STRING:
                row.addString(columnName, columnValue);
                break;
            default:
                throw new IllegalStateException(String.format("unknown column type %s", columnType));
        }
    }
}
