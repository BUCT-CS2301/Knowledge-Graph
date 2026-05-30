import pymysql
from neo4j import GraphDatabase

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

def get_mysql_connection():
    conn = pymysql.connect(**MYSQL_CONFIG)
    conn.cursor().execute("SET NAMES utf8mb4")
    return conn

def get_neo4j_driver():
    return GraphDatabase.driver(**NEO4J_CONFIG)

def fetch_artifacts_with_museum(conn):
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    query = """
        SELECT a.object_id, a.title, a.period, a.type, a.material,
               a.description, a.dimensions, a.credit_line, a.accession_number,
               m.name AS museum_name, m.name_cn AS museum_name_cn,
               m.location AS museum_location
        FROM artifact a
        LEFT JOIN museum m ON a.museum_id = m.object_id
        WHERE a.is_deleted = 0
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    return results

def clear_neo4j(driver):
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    print("Neo4j data cleared")

def create_indexes(driver):
    index_queries = [
        "CREATE INDEX IF NOT EXISTS FOR (a:Artifact) ON (a.object_id)",
        "CREATE INDEX IF NOT EXISTS FOR (oid:ObjectID) ON (oid.value)",
        "CREATE INDEX IF NOT EXISTS FOR (p:Period) ON (p.name)",
        "CREATE INDEX IF NOT EXISTS FOR (mat:Material) ON (mat.name)",
        "CREATE INDEX IF NOT EXISTS FOR (t:ArtifactType) ON (t.name)",
        "CREATE INDEX IF NOT EXISTS FOR (m:Museum) ON (m.name)",
        "CREATE INDEX IF NOT EXISTS FOR (l:Location) ON (l.name)",
    ]
    with driver.session() as session:
        for query in index_queries:
            session.run(query)

def create_neo4j_triples(driver, artifacts):
    with driver.session() as session:
        total = len(artifacts)
        for idx, artifact in enumerate(artifacts, 1):
            if idx % 50 == 0 or idx == total:
                print(f"Processing artifact {idx}/{total}...")

            artifact_id = artifact['object_id']
            title = (artifact.get('title') or '').strip()
            if not title:
                title = f"未命名文物_{artifact_id}"

            period = (artifact.get('period') or '').strip()
            material = (artifact.get('material') or '').strip()
            type_val = (artifact.get('type') or '').strip()
            museum_name_cn = (artifact.get('museum_name_cn') or '').strip()
            museum_name_en = (artifact.get('museum_name') or '').strip()
            museum_name = museum_name_cn or museum_name_en
            museum_location = (artifact.get('museum_location') or '').strip()
            dimensions = (artifact.get('dimensions') or '').strip()
            description = (artifact.get('description') or '').strip()
            credit_line = (artifact.get('credit_line') or '').strip()
            accession_number = (artifact.get('accession_number') or '').strip()

            # 只MERGE一次Artifact节点
            session.run("""
                MERGE (a:Artifact {object_id: $artifact_id})
                ON CREATE SET a.title = $title,
                              a.dimensions = $dimensions,
                              a.description = $description,
                              a.credit_line = $credit_line,
                              a.accession_number = $accession_number
                ON MATCH SET  a.title = $title,
                              a.dimensions = $dimensions,
                              a.description = $description,
                              a.credit_line = $credit_line,
                              a.accession_number = $accession_number
            """, {
                'title': title,
                'artifact_id': artifact_id,
                'dimensions': dimensions,
                'description': description[:5000],
                'credit_line': credit_line,
                'accession_number': accession_number
            })

            # 编号关系（添加WITH子句）
            session.run("""
                MERGE (oid:ObjectID {value: $artifact_id})
                WITH oid
                MATCH (a:Artifact {object_id: $artifact_id})
                MERGE (a)-[:编号]->(oid)
            """, {'artifact_id': artifact_id})

            # 所属朝代关系
            if period:
                period_lines = [p.strip() for p in period.replace('\r\n', '\n').split('\n') if p.strip()]
                for p in period_lines:
                    session.run("""
                        MERGE (per:Period {name: $period})
                        WITH per
                        MATCH (a:Artifact {object_id: $artifact_id})
                        MERGE (a)-[:所属朝代]->(per)
                    """, {'period': p, 'artifact_id': artifact_id})

            # 制作材质关系
            if material:
                materials = [m.strip() for m in material.replace(';', '|').split('|') if m.strip()]
                for mat in materials:
                    session.run("""
                        MERGE (mat_node:Material {name: $material})
                        WITH mat_node
                        MATCH (a:Artifact {object_id: $artifact_id})
                        MERGE (a)-[:制作材质]->(mat_node)
                    """, {'material': mat, 'artifact_id': artifact_id})

            # 文物品类关系
            if type_val:
                types = [t.strip() for t in type_val.replace(';', '|').replace('、', '|').split('|') if t.strip()]
                for t in types:
                    session.run("""
                        MERGE (type_node:ArtifactType {name: $type})
                        WITH type_node
                        MATCH (a:Artifact {object_id: $artifact_id})
                        MERGE (a)-[:文物品类]->(type_node)
                    """, {'type': t, 'artifact_id': artifact_id})

            # 收藏馆藏关系
            if museum_name:
                session.run("""
                    MERGE (m:Museum {name: $museum_name})
                    ON CREATE SET m.name_en = $museum_name_en
                    ON MATCH SET  m.name_en = $museum_name_en
                    WITH m
                    MATCH (a:Artifact {object_id: $artifact_id})
                    MERGE (a)-[:收藏馆藏]->(m)
                """, {
                    'museum_name': museum_name,
                    'museum_name_en': museum_name_en,
                    'artifact_id': artifact_id
                })

                # 坐落地址关系
                if museum_location:
                    session.run("""
                        MERGE (l:Location {name: $location})
                        WITH l
                        MATCH (m:Museum {name: $museum_name})
                        MERGE (m)-[:坐落地址]->(l)
                    """, {'location': museum_location, 'museum_name': museum_name})

def main():
    mysql_conn = None
    neo4j_driver = None
    try:
        mysql_conn = get_mysql_connection()
        print("Connected to MySQL successfully")

        artifacts = fetch_artifacts_with_museum(mysql_conn)
        print(f"Fetched {len(artifacts)} artifacts")

        neo4j_driver = get_neo4j_driver()
        print("Connected to Neo4j successfully")

        clear_neo4j(neo4j_driver)

        print("Creating indexes...")
        create_indexes(neo4j_driver)
        print("Indexes created")

        print("Creating triples...")
        create_neo4j_triples(neo4j_driver, artifacts)
        print("Triples created successfully")

    except pymysql.MySQLError as e:
        print(f"MySQL error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if mysql_conn:
            mysql_conn.close()
            print("MySQL connection closed")
        if neo4j_driver:
            neo4j_driver.close()
            print("Neo4j driver closed")

if __name__ == '__main__':
    main()