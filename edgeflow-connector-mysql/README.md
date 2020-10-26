edgeflow-connector-mysql
===========================

## Input

### 配置规范

|配置前缀|说明|
|---------|---------------------------|
| datasource.url | JDBC URL|
| datasource.username | JDBC 用户名|
| datasource.password | JDBC 密码 |
| table-or-query | 表名或查询 |
| mode | 读取的方式：bulk（全量） / id（根据自增ID增量） / timestamp(根据时间戳增量)/ id-timestamp(根据ID和时间戳增量) |
| offset.manage | true / false 是否管理 offset |
| offset.output | offset 输出数据源 |
| offset.cols.id | ID 列 |
| offset.cols.timestamp | 时间戳列  |
| partition.num | 分片数量 |
| partition.lower-bound | 分片下界 |
| partition.upper-bound | 分片上界 |
| parameter.* | 自定义参数 |

## Output 

### 配置规范

|配置前缀|说明|
|---------|---------------------------|
| datasource.url | JDBC URL|
| datasource.username | JDBC 用户名|
| datasource.password | JDBC 密码 |
| table | 表名 |