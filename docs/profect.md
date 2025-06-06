数据来源:业务数据
        日志数据:曝光日志,动作日志,页面日志,启动日志,错误日志
技术栈:
     数据采集:flinkcdc,flume
     数据存储:hbase,hive,doris,kafka
flink概念:高性能,弹性,高可用,高并发,高吞吐。
和flinkcdc相似的采集软件有:maxwell：mysql数据库的binlog数据，将binlog数据转换成json数据，并写入kafka。

状态去重:用到了取反过滤,将数据去重,避免重复消费。因为flinkcdc是增量采集,所以不能保证数据不重复。

doris物化图概念:。Doris 物化视图是一种将查询结果预先存储起来的特殊表，它既包含计算逻辑也包含数据。其核心作用是通过预计算和存储数据，来加速查询并节省计算资源
使用场景：
决策支持系统：在 BI 报表、Ad - Hoc 查询等场景中，可对常见的包含聚合操作、可能涉及多表连接的分析型查询进行加速。
数据分层场景：通过嵌套物化视图来构建 DWD（数据仓库明细层）和 DWM（数据仓库中间层）层，利用其调度刷新能力。
核心价值
查询加速：能将原本可能需要分钟级的复杂查询优化至秒级响应，极大地提升了查询效率。
资源节省：减少了大量重复计算所消耗的资源，可节省约 80% 的计算资源。
架构简化：可替代传统的 ETL 流程来构建实时数仓，简化了数据处理架构。
         
1.ods层,用flinkcdc采取mysql业务数据数据,判断数据是否是json格式并且判断after是否为空,然后存储到kafka,效果如下:
![img.png](imgs/img.png)
2.dim层从kafka读取业务层原始数据,然后用flinkcdc采集mysql的维度数据,
然后用广播连接,将维度数据广播给业务数据,然后进行join,将维度数据与业务数据进行join,将结果写入到hbase,效果如下:
![img_1.png](imgs/img_1.png)
3.新老用户,从kafka获取页面日志信息,然后使用工具类读取kafka,将日志信息分流分别存入到kafka,提取启动日志信息做新老用户,
//判断 is_new 是否为 "1"。
//如果 s1 为 "1"，将状态更新为今天的日期。
//如果 s1 不是今天的日期，将 is_new 字段更新为 "0"，表示不是新用户。
//如果状态为空，将状态更新为昨天的日期。,写入到kafuka：
![img.png](imgs/img1.png)
4.flinksql:ods层原始数据连接维度hbase相关的数据写入到kafka
![img.png](imgs/img2.png)
![img_1.png](imgs/img_3.png)
5.dws读取kafka数据连接hbase数据写入到doris
(1)sku粒度下单业务过程聚合统计
![img.png](imgs/img_4.png)
![img.png](imgs/img_5.png)
(2)按照版本、地区、渠道、新老访客对pv、uv、sv、dur进行聚合统计：
![img.png](imgs/img_7.png)
(3)统计各省份订单金额
![img.png](imgs/img_8.png)
6.报表,统计各省份下单金额:按照省份分区,统计出各省份订单金额。
各品牌订单金额:按照品牌分区,统计出各品牌订单金额。
各商品下单总金额:按照商品名称分区,统计出各商品下单总金额。
个省份下单次数:按照省份分区,统计出各省份下单数量。
搜索词频:按照搜索词分区,统计出搜索词出现的次数。
![img.png](imgs/img_6.png)

//oracle建表  
-- 创建名为ZYSERVICESPACE的表空间，用于存储数据库对象
CREATE TABLESPACE ZYSERVICESPACE
-- 指定数据文件的路径，需确保该路径存在且Oracle用户有读写权限
DATAFILE 'E:\orcl\pdbseed'
-- 初始大小为1024MB
SIZE 1024M
-- 启用自动扩展功能
AUTOEXTEND ON
-- 每次扩展的增量为100MB
NEXT 100M
-- 最大大小无限制（生产环境建议设置合理上限）
MAXSIZE UNLIMITED;

-- 创建数据库用户ZYDEVER，密码为zydev2021
CREATE USER ZYDEVER
IDENTIFIED BY "zydev2021"
-- 指定默认表空间为ZYSERVICESPACE
DEFAULT TABLESPACE ZYSERVICESPACE
-- 使用默认的资源限制配置文件
PROFILE DEFAULT
-- 创建后立即解锁账户
ACCOUNT UNLOCK;

-- 授予用户基本权限：
-- CONNECT角色：允许用户连接到数据库
-- RESOURCE角色：允许用户创建表、序列等基本对象
GRANT CONNECT, RESOURCE TO ZYDEVER;

-- 授予用户无限制使用表空间的权限（不推荐生产环境）
GRANT UNLIMITED TABLESPACE TO ZYDEVER;

-- 在ZYDEVER模式下创建名为name的表（存储员工信息）
CREATE TABLE ZYDEVER.name (
-- 员工ID，数字类型(长度10)，设为主键
emp_id NUMBER(10) PRIMARY KEY,
-- 员工姓名，字符串类型(最大50字符)，不可为空
emp_name VARCHAR2(50) NOT NULL,
-- 部门名称，字符串类型(最大50字符)
department VARCHAR2(50),
-- 薪水，数字类型(总长度10，小数位2)
salary NUMBER(10, 2),
-- 入职日期，默认为当前系统日期
hire_date DATE DEFAULT SYSDATE
);

-- 插入一条员工记录（指定列名）
INSERT INTO ZYDEVER.name (emp_id, emp_name, department, salary)
VALUES (1, 'John Doe', 'IT', 5000.00);

-- 插入另一条员工记录（按表结构顺序提供所有字段值）
INSERT INTO ZYDEVER.name
VALUES (2, 'Jane Smith', 'HR', 6000.00, TO_DATE('2023-01-15', 'YYYY-MM-DD'));

-- 查询ZYDEVER模式下的name表中的所有记录
SELECT * FROM ZYDEVER.name;


-- 构建基础查询获取员工信息
WITH base_data AS (
SELECT emp_id, emp_name, department, salary
FROM ZYDEVER.name
)
-- 使用PIVOT进行行转列操作
SELECT *
FROM base_data
PIVOT (
-- 聚合函数，这里以获取最大薪水为例（实际根据需求调整聚合逻辑）
MAX(salary) AS max_salary,
MAX(department) AS dept
FOR emp_name IN ('John Doe' AS john_doe, 'Jane Smith' AS jane_smith)
);






CREATE TABLE ZYDEVER.EMP_SUMMARY (
emp_id   NUMBER,
emp_name VARCHAR2(50),
dept_10  NUMBER,  -- 部门10的薪资
dept_20  NUMBER,  -- 部门20的薪资
dept_30  NUMBER   -- 部门30的薪资
);

INSERT INTO ZYDEVER.EMP_SUMMARY VALUES (1, 'John', 5000, 5500, 6000);
INSERT INTO ZYDEVER.EMP_SUMMARY VALUES (2, 'Jane', 6500, 7000, 7500);

select * from ZYDEVER.EMP_SUMMARY;

SELECT emp_id, emp_name, department, salary
FROM ZYDEVER.EMP_SUMMARY
UNPIVOT (
salary               -- 转换后的值列
FOR department IN (  -- 转换后的名列
dept_10 AS '10',
dept_20 AS '20',
dept_30 AS '30'
)
);




