import pymysql
import argparse
from neo4j import GraphDatabase

# ===================== 配置项 =====================
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '643114514',
    'database': 'admin_platform',
    'charset': 'utf8mb4'
}

NEO4J_CONFIG = {
    'uri': 'bolt://localhost:7687',
    'auth': ('neo4j', 'password123')
}

# ===================== 命令行参数解析 =====================
def parse_args():
    parser = argparse.ArgumentParser(description='文物数据同步到Neo4j')
    parser.add_argument('--keep', action='store_true', help='启用增量更新（仅新增，不清空）')
    return parser.parse_args()

# ===================== 数据库连接 =====================
def get_mysql_connection():
    conn = pymysql.connect(**MYSQL_CONFIG)
    conn.cursor().execute("SET NAMES utf8mb4")
    return conn

def get_neo4j_driver():
    return GraphDatabase.driver(**NEO4J_CONFIG)

# ===================== MySQL 查询工具 =====================
def get_mysql_all_accessions(conn):
    """获取 MySQL 所有【未删除】文物的 馆藏号(accession_number)"""
    cursor = conn.cursor()
    cursor.execute("SELECT accession_number FROM artifact WHERE is_deleted = 0")
    return {row[0] for row in cursor.fetchall() if row[0]}

def get_mysql_deleted_artifacts(conn):
    """获取 MySQL 已软删除的文物 object_id（用于删除Neo4j）"""
    cursor = conn.cursor()
    cursor.execute("SELECT object_id FROM artifact WHERE is_deleted = 1")
    return [row[0] for row in cursor.fetchall()]

def fetch_artifacts_by_accessions(conn, accession_list):
    """根据 馆藏号 批量查询文物数据（增量专用）"""
    if not accession_list:
        return []
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    placeholders = ','.join(['%s'] * len(accession_list))
    query = f"""
        SELECT 
            a.object_id, a.title, a.period, a.type, a.material,
            a.description, a.dimensions, a.credit_line, a.accession_number,
            a.image_url, a.detail_url,
            m.name AS museum_name, m.name_cn AS museum_name_*****************.location AS museum_location
        FROM artifact a
        LEFT JOIN museum m ON a.museum_id = m.object_id
        WHERE a.accession_number IN ({placeholders})
    """
    cursor.execute(query, tuple(accession_list))
    results = cursor.fetchall()
    cursor.close()
    return results

def fetch_all_artifacts(conn):
    """全量查询所有未删除文物（默认模式）"""
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    query = """
        SELECT 
            a.object_id, a.title, a.period, a.type, a.material,
            a.description, a.dimensions, a.credit_line, a.accession_number,
            a.image_url, a.detail_url,
            m.name AS museum_name, m.name_cn AS museum_name_*****************.location AS museum_location
        FROM artifact a
        LEFT JOIN museum m ON a.museum_id = m.object_id
        WHERE a.is_deleted = 0
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    return results

# ===================== Neo4j 查询工具 =====================
def get_neo4j_accessions(driver):
    """获取 Neo4j 中已存在的文物 馆藏号(accession_number)"""
    with driver.session() as session:
        result = session.run("MATCH (a:Artifact) RETURN a.accession_number AS acc")
        return {record["acc"] for record in result if record["acc"]}

# ===================== Neo4j 操作 =====================
def clear_neo4j(driver):
    """清空Neo4j全量数据（默认模式）"""
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    print("✅ Neo4j 已清空所有数据")

def delete_neo4j_artifacts(driver, object_ids):
    """根据MySQL的object_id删除Neo4j已软删除的文物"""
    if not object_ids:
        return
    with driver.session() as session:
        for oid in object_ids:
            session.run("MATCH (a:Artifact {object_id: $oid}) DETACH DELETE a", {"oid": oid})
    print(f"✅ 同步删除 {len(object_ids)} 个已软删除文物")

def create_indexes(driver):
    """创建索引（幂等）"""
    index_queries = [
        "CREATE INDEX IF NOT EXISTS FOR (a:Artifact) ON (a.object_id)",
        "CREATE INDEX IF NOT EXISTS FOR (a:Artifact) ON (a.accession_number)",
        "CREATE INDEX IF NOT EXISTS FOR (p:Period) ON (p.name)",
        "CREATE INDEX IF NOT EXISTS FOR (mat:Material) ON (mat.name)",
        "CREATE INDEX IF NOT EXISTS FOR (t:ArtifactType) ON (t.name)",
        "CREATE INDEX IF NOT EXISTS FOR (m:Museum) ON (m.name)",
    ]
    with driver.session() as session:
        for q in index_queries:
            session.run(q)

def safe_str(val):
    """安全字符串处理（和你的CSV代码一致）"""
    return str(val).strip() if val else ""

def create_neo4j_triples(driver, artifacts):
    """创建知识图谱（MERGE保证不重复）"""
    with driver.session() as session:
        total = len(artifacts)
        for idx, art in enumerate(artifacts, 1):
            if idx % 50 == 0 or idx == total:
                print(f"处理进度：{idx}/{total}")

            # 核心字段（MySQL自动UUID + 馆藏号唯一）
            object_id = safe_str(art['object_id'])
            accession = safe_str(art['accession_number'])
            title = safe_str(art.get('title')) or f"未命名文物_{object_id}"
            period = safe_str(art.get('period'))
            art_type = safe_str(art.get('type'))
            material = safe_str(art.get('material'))
            dimensions = safe_str(art.get('dimensions'))
            description = safe_str(art.get('description'))[:5000]
            credit_line = safe_str(art.get('credit_line'))
            image_url = safe_str(art.get('image_url'))
            detail_url = safe_str(art.get('detail_url'))

            # 博物馆字段
            museum_cn = safe_str(art.get('museum_name_cn'))
            museum_en = safe_str(art.get('museum_name'))
            museum_name = museum_cn or museum_en
            museum_loc = safe_str(art.get('museum_location'))

            # 1. 创建/更新文物主节点
            session.run("""
                MERGE (a:Artifact {object_id: $oid})
                ON CREATE SET a.title = $title, a.accession_number = $acc, a.period = $period,
                              a.type = $type, a.material = $mat, a.dimensions = $dim,
                              a.description = $desc, a.credit_line = $credit,
                              a.image_url = $img, a.detail_url = $detail
            """, {
                "oid": object_id, "acc": accession, "title": title, "period": period,
                "type": art_type, "mat": material, "dim": dimensions, "desc": description,
                "credit": credit_line, "img": image_url, "detail": detail_url
            })

            # 2. 所属朝代
            if period:
                session.run("""
                    MERGE (p:Period {name: $p})
                    WITH p MATCH (a:Artifact {object_id: $oid})
                    MERGE (a)-[:所属朝代]->(p)
                """, {"p": period, "oid": object_id})

            # 3. 制作材质
            if material:
                for mat in [x.strip() for x in material.replace(';','|').split('|') if x.strip()]:
                    session.run("""
                        MERGE (m:Material {name: $mat})
                        WITH m MATCH (a:Artifact {object_id: $oid})
                        MERGE (a)-[:制作材质]->(m)
                    """, {"mat": mat, "oid": object_id})

            # 4. 文物品类
            if art_type:
                for t in [x.strip() for x in art_type.replace(';','|').replace('、','|').split('|') if x.strip()]:
                    session.run("""
                        MERGE (t:ArtifactType {name: $t})
                        WITH t MATCH (a:Artifact {object_id: $oid})
                        MERGE (a)-[:文物品类]->(t)
                    """, {"t": t, "oid": object_id})

            # 5. 收藏馆藏 + 地址
            if museum_name:
                session.run("""
                    MERGE (m:Museum {name: $name})
                    ON CREATE SET m.name_en = $en
                    WITH m MATCH (a:Artifact {object_id: $oid})
                    MERGE (a)-[:收藏馆藏]->(m)
                """, {"name": museum_name, "en": museum_en, "oid": object_id})

                if museum_loc:
                    session.run("""
                        MERGE (l:Location {name: $loc})
                        WITH l MATCH (m:Museum {name: $name})
                        MERGE (m)-[:坐落地址]->(l)
                    """, {"loc": museum_loc, "name": museum_name})

# ===================== 主函数 =====================
def main():
    args = parse_args()
    mysql_conn = neo4j_driver = None

    try:
        mysql_conn = get_mysql_connection()
        neo4j_driver = get_neo4j_driver()
        print("✅ 数据库连接成功")

        create_indexes(neo4j_driver)

        # ============== 核心逻辑 ==============
        if not args.keep:
            # 【默认全量模式】清空Neo4j + 全量导入
            print("🔄 全量覆盖模式：清空Neo4j")
            clear_neo4j(neo4j_driver)
            artifacts = fetch_all_artifacts(mysql_conn)
        else:
            # 【--keep 增量模式】仅同步：MySQL有、Neo4j没有的馆藏号
            print("🔄 增量更新模式：仅新增数据")
            mysql_acc = get_mysql_all_accessions(mysql_conn)
            neo4j_acc = get_neo4j_accessions(neo4j_driver)
            new_acc = mysql_acc - neo4j_acc  # 差集：需要新增的馆藏号
            print(f"📊 检测到 {len(new_acc)} 个新增文物")
            artifacts = fetch_artifacts_by_accessions(mysql_conn, new_acc)

        # 同步数据
        if artifacts:
            create_neo4j_triples(neo4j_driver, artifacts)

        # 同步删除MySQL软删除的数据
        deleted_ids = get_mysql_deleted_artifacts(mysql_conn)
        delete_neo4j_artifacts(neo4j_driver, deleted_ids)

        print("\n🎉 数据同步完成！")

    except Exception as e:
        print(f"❌ 错误：{str(e)}")
    finally:
        if mysql_conn:
            mysql_conn.close()
        if neo4j_driver:
            neo4j_driver.close()
        print("🔌 连接已关闭")

if __name__ == '__main__':
    main()