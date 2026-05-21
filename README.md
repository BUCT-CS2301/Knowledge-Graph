# Knowledge-Graph

中国文物知识图谱系统，数据来源于三家海外博物馆的公开藏品。

## 项目结构

```
Knowledge-Graph/
├── spider/                  # 爬虫：抓取博物馆藏品数据
│   ├── cleveland_museum/
│   ├── nelson-atkins_museum/
│   └── penn_museum/
├── Data Cleaning/           # 数据清洗：处理原始 CSV
├── Translation/             # 翻译：将英文字段翻译为中文
├── CreateTriple/            # 构建知识图谱
│   ├── createSQL.py         # CSV → MySQL
│   └── createTriple.py      # MySQL → Neo4j 三元组
├── mysql/
│   └── init.sql             # MySQL 建表脚本
└── docker-compose.dev.yml   # Docker 开发环境
```

## 环境启动

```bash
docker compose -f docker-compose.dev.yml up -d
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

# 默认：先清空 artifact 和 museum 表，再导入
python createSQL.py

# 保留现有数据，追加导入（按 accession_number 去重）
python createSQL.py --keep
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
| Image | url | 展示图片 |

### 关系类型

| 关系 | 起点 → 终点 | 说明 |
|------|-------------|------|
| 编号 | Artifact → ObjectID | 文物的唯一编号 |
| 所属朝代 | Artifact → Period | 文物所属朝代 |
| 制作材质 | Artifact → Material | 文物制作材质（支持多材质，按 `\|` `;` 分割） |
| 文物品类 | Artifact → ArtifactType | 文物类别（支持多品类，按 `\|` `;` `、` 分割） |
| 收藏馆藏 | Artifact → Museum | 收藏该文物的博物馆 |
| 坐落地址 | Museum → Location | 博物馆所在地址 |
| 展示图片 | Artifact → Image | 文物展示图片 |

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
