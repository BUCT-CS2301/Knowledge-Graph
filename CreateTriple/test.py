from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError
import pymysql
from pymysql import OperationalError

HOST = "39.106.231.119"

# ========== 测试 Neo4j ==========
def test_neo4j():
    uri = f"bolt://{HOST}:7687"
    user = "neo4j"
    pwd = "password123"
    print("\n===== 开始测试 Neo4j =====")
    try:
        driver = GraphDatabase.driver(uri, auth=(user, pwd))
        with driver.session() as session:
            session.run("RETURN 1")
        print("Neo4j 连接+认证成功 ✅")
        driver.close()
    except ServiceUnavailable:
        print("Neo4j 无法访问：端口/网络/服务异常 ❌")
    except AuthError:
        print("Neo4j 账号密码错误 ❌")
    except Exception as e:
        print(f"Neo4j 未知错误：{e}")

# ========== 测试 MySQL ==========
def test_mysql():
    port = 3306
    user = "root"
    pwd = "643114514"
    print("\n===== 开始测试 MySQL =====")
    try:
        conn = pymysql.connect(
            host=HOST,
            port=port,
            user=user,
            password=pwd,
            connect_timeout=5
        )
        print("MySQL 连接+认证成功 ✅")
        conn.close()
    except OperationalError as e:
        if "Access denied" in str(e):
            print("MySQL 账号密码错误 ❌")
        else:
            print("MySQL 无法访问：端口/防火墙/服务异常 ❌")
    except Exception as e:
        print(f"MySQL 未知错误：{e}")

if __name__ == "__main__":
    test_neo4j()
    test_mysql()