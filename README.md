# Knowledge-Graph

中国文物知识图谱系统，数据来源于三家海外博物馆的公开藏品。

## 项目结构

```
Knowledge-Graph/
├── spider/                          # 爬虫：抓取博物馆藏品数据
│   ├── cleveland_museum/
│   │   ├── cleveland_museum.py      # 克利夫兰博物馆爬虫
│   │   └── cleveland_museum.csv     # 爬取结果
│   ├── nelson-atkins_museum/
│   │   ├── nelson-atkins_museum.py  # 尼尔森-阿特金斯博物馆爬虫
│   │   └── nelson-atkins_museum.csv
│   ├── penn_museum/
│   │   ├── penn_museum.py           # 宾大博物馆爬虫
│   │   └── penn_museum.csv
│   └── csv文档数据说明.md
├── Data Cleaning/                   # 数据清洗：处理原始 CSV
│   ├── cleveland.py                 # 克利夫兰数据清洗脚本
│   ├── cleveland.csv                # 清洗后数据
│   ├── nelson_atkins.py
│   ├── nelson_atkins.csv
│   ├── penn.py
│   └── penn.csv
├── Translation/                     # 翻译：将英文字段翻译为中文
│   ├── translate_cleveland.py       # 克利夫兰翻译脚本
│   ├── input_cleveland.csv          # 翻译输入
│   ├── translated_cleveland.csv     # 翻译输出（中文）
│   ├── translate_nelson_atkins.py
│   ├── input_nelson_atkins.csv
│   ├── translated_nelson_atkins.csv
│   ├── translate_penn.py
│   ├── input_penn.csv
│   ├── translated_penn.csv
│   └── date.py                      # 日期格式处理
├── CreateTriple/                    # 构建知识图谱
│   ├── createSQL.py                 # CSV → MySQL（支持 --keep 追加模式）
│   ├── test.py                      # 测试 Neo4j以及MySQL 连接
│   ├── check.py                     # 检查 有效数据数量及删除无效数据s
│   └── createTriple.py              # MySQL → Neo4j 三元组
├── mysql/
│   ├── init.sql                     # MySQL 建表脚本
│   └── alter.sql                    # 数据库变更脚本
├── Meeting/                         # 会议记录
│   ├── week8.md
│   ├── week9.md
│   ├── week10.md
│   └── week11.md
├── README.md
└── docker-compose.dev.yml           # Docker 开发环境
```

## 环境启动

```bash
docker compose -f docker-compose.dev.yml up -d
补充说明：响应更改需求，新增role表的permissions字段,增加方式为执行alter.sql脚本
```

包含服务：
- MySQL 8.0（端口 3306，已配置 `mysql_native_password` 认证）
- Neo4j 5（端口 7474/7687）
- Redis 7（端口 6379）
- MinIO（端口 9000/9001）

## 数据导入流程

### 1. CSV → MySQL

```bash
cd CreateTriple
# 测试 Neo4j 连接
python test.py

# 默认：先清空 artifact 和 museum 表，再导入
python createSQL.py

# 保留现有数据，追加导入（按 accession_number 去重）
python createSQL.py --keep

# 检查并无效数据
python check.py

#删除无效数据
python check.py --delete

```

- 数据源：`Translation/translated_*.csv`（三家博物馆的中文翻译数据）
- 优先使用 `type_cn` 列作为文物品类，不存在时回退到 `type` 列
- `--keep` 模式下，已存在的 `accession_number` 会自动跳过

### 2. MySQL → Neo4j 知识图谱

```bash
python createTriple.py
```

- 从 MySQL 读取文物数据，构建 Neo4j 三元组
- 每次运行会**清空 Neo4j 全部数据**后重建

## 数据库表结构

数据库名：`admin_platform`，字符集：`utf8mb4`，排序规则：`utf8mb4_unicode_ci`

**museum（博物馆表）**

| 字段 | 类型 | 约束 | 说明 |
|------|------|------|------|
| object_id | VARCHAR(36) | PK | 主键 |
| name | VARCHAR(200) | NOT NULL | 英文名 |
| name_cn | VARCHAR(200) | | 中文名 |
| location | VARCHAR(200) | | 地址 |
| website | VARCHAR(500) | | 官网 |

**artifact（文物表）**

| 字段 | 类型 | 约束 | 说明 |
|------|------|------|------|
| object_id | VARCHAR(36) | PK | 主键（来自CSV原始ID） |
| title | VARCHAR(500) | NOT NULL | 文物名称 |
| period | VARCHAR(200) | | 朝代/时期 |
| type | VARCHAR(100) | | 文物品类（优先type_cn） |
| material | VARCHAR(200) | | 材质 |
| description | TEXT | | 描述 |
| dimensions | VARCHAR(300) | | 尺寸 |
| museum_id | VARCHAR(36) | FK → museum | 所属博物馆 |
| detail_url | VARCHAR(1000) | NOT NULL | 详情页链接 |
| image_url | VARCHAR(1000) | NOT NULL | 图片链接 |
| image_path | VARCHAR(500) | | 本地图片路径 |
| credit_line | VARCHAR(500) | | 来源/捐赠信息 |
| accession_number | VARCHAR(100) | | 馆藏编号 |
| crawl_date | DATE | NOT NULL | 采集日期 |
| create_time | DATETIME | NOT NULL, DEFAULT CURRENT_TIMESTAMP | 创建时间 |
| update_time | DATETIME | ON UPDATE CURRENT_TIMESTAMP | 更新时间 |
| is_deleted | TINYINT(1) | DEFAULT 0 | 逻辑删除 |

## 知识图谱结构

### 节点类型

| 节点 | 主键 | 说明 |
|------|------|------|
| Artifact | title + object_id | 文物，以名称为标识 |
| ObjectID | value | 文物编号 |
| Period | name | 朝代/时期 |
| Material | name | 材质 |
| ArtifactType | name | 文物品类 |
| Museum | name | 博物馆 |
| Location | name | 地理位置 |


### 关系类型

| 关系 | 起点 → 终点 | 说明 |
|------|-------------|------|
| 编号 | Artifact → ObjectID | 文物的唯一编号 |
| 所属朝代 | Artifact → Period | 文物所属朝代 |
| 制作材质 | Artifact → Material | 文物制作材质（支持多材质，按 `\|` `;` 分割） |
| 文物品类 | Artifact → ArtifactType | 文物类别（支持多品类，按 `\|` `;` `、` 分割） |
| 收藏馆藏 | Artifact → Museum | 收藏该文物的博物馆 |
| 坐落地址 | Museum → Location | 博物馆所在地址 |


### Artifact 节点属性

`title`、`object_id`、`dimensions`、`description`、`credit_line`、`accession_number`

## 数据库连接配置

| 服务 | 地址 | 用户名 | 密码 |
|------|------|--------|------|
| MySQL | localhost:3306 | root | 643114514 |
| Neo4j | bolt://localhost:7687 | neo4j | password123 |
| Redis | localhost:6379 | - | - |
| MinIO | localhost:9000 | minioadmin | minioadmin123 |

## 数据来源

| 博物馆 | 英文名 | 位置 |
|--------|--------|------|
| 克利夫兰艺术博物馆 | Cleveland Museum of Art | 美国俄亥俄州克利夫兰 |
| 尼尔森-阿特金斯艺术博物馆 | Nelson-Atkins Museum of Art | 美国堪萨斯城 |
| 宾夕法尼亚大学考古与人类学博物馆 | Penn Museum | 美国宾夕法尼亚州费城 |
