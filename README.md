# Flink 入门学习

## Flink batch demo

1. 创建执行环境
2. 从文件读取数据
3. 切分、转换（word, 1）
4. 按照 word 分组
5. 按分组聚合
6. 输出

## Flink stream demo

1. 创建执行环境
2. 从 socket 读取数据
3. 处理数据
4. 输出
5. 执行

### Note

1. 当 lambda 表达式有泛型时，需要显示指定类型 .returns(Types.{type1}, Types.{type2},...)
2. 端口监听指令 `nc -lk {port}`, `nc` 是 `netcat`
