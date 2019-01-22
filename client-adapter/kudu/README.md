## 基本说明
* alibaba canal的基础上添加了client adapter kudu
* alibaba canal 源码 https://github.com/alibaba/canal

## 使用说明
* 使用方法参考client adapter文档，只需要修改配置即可

## kudu client adapter 配置
```
dataSourceKey: defaultDS
destination: example
kuduMapping:
  database: didapinche_taxi #mysql database
  table: taxi_ride #mysql table
  targetTable: k_didapinche_taxi.taxi_ride #kudu对应的表
  # mysql field 作为kudu的 row key
  targetKeys:
    id: id$INT64
  # mysql field : kudu field${kudu type}
  targetColumns:
    name: name$STRING
    driver_id: driver_id$INT64
    create_time: create_time$STRING

#kudu 类型
#BOOL:
#FLOAT:
#DOUBLE:
#BINARY:
#INT8:
#INT16:
#INT32:
#INT64:
#STRING:
```


