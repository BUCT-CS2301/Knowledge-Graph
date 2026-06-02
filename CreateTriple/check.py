import pymysql
import os
import argparse
from pymysql.cursors import DictCursor

DB_CONFIG = {
    'host': '39.106.231.119',
    'port': 3306,
    'user': 'root',
    'password': '643114514',
    'database': 'admin_platform',
    'charset': 'utf8mb4',
    'cursorclass': DictCursor
}

def check_image_files(cursor):
    """检查artifact表中的image_path对应的文件是否存在，返回统计结果和缺失的object_id列表"""
    total_count = 0
    exist_count = 0
    missing_count = 0
    missing_object_ids = []
    missing_files = []
    
    # 查询所有记录（分批查询，避免大数据量内存溢出）
    offset = 0
    batch_size = 1000
    
    print("[INFO] 开始分批查询数据库记录...")
    while True:
        cursor.execute("SELECT object_id, image_path, title FROM artifact LIMIT %s OFFSET %s", (batch_size, offset))
        results = cursor.fetchall()
        if not results:
            break
            
        batch_count = len(results)
        total_count += batch_count
        
        # 检查当前批次
        for row in results:
            object_id = row['object_id']
            image_path = row['image_path']
            title = row['title'] or '无标题'
            
            if os.path.isfile(image_path):
                exist_count += 1
            else:
                missing_count += 1
                missing_object_ids.append(object_id)
                missing_files.append({
                    'object_id': object_id,
                    'image_path': image_path,
                    'title': title
                })
        
        # 显示进度
        print(f"[PROGRESS] 已检查 {total_count} 条记录，当前发现 {missing_count} 个缺失文件")
        offset += batch_size
    
    # 输出统计结果
    print("\n" + "="*60)
    print("📊 图片文件检查统计结果")
    print("="*60)
    print(f"总记录数: {total_count}")
    print(f"存在文件数: {exist_count}")
    print(f"缺失文件数: {missing_count}")
    print(f"缺失率: {missing_count/total_count*100:.2f}%" if total_count > 0 else "缺失率: 0.00%")
    print("="*60)
    
    # 输出前10个缺失文件示例
    if missing_count > 0:
        print("\n❌ 缺失的文件示例（前10个）:")
        for i, file_info in enumerate(missing_files[:10]):
            print(f"  {i+1}. 标题: {file_info['title'][:30]}...")
            print(f"     object_id: {file_info['object_id']}")
            print(f"     路径: {file_info['image_path']}")
        
        if missing_count > 10:
            print(f"     ... 还有 {missing_count-10} 个缺失文件")
    
    return {
        'total': total_count,
        'exist': exist_count,
        'missing': missing_count,
        'missing_object_ids': missing_object_ids,
        'missing_files': missing_files
    }

def delete_missing_artifacts(cursor, missing_object_ids):
    """批量删除缺失图片的artifact记录"""
    if not missing_object_ids:
        print("\n[INFO] 没有需要删除的记录")
        return 0
    
    missing_count = len(missing_object_ids)
    print(f"\n[WARNING] 即将删除 {missing_count} 条无效记录！")
    print("[WARNING] 此操作不可撤销，请确认！")
    
    # 批量删除（分批次避免SQL语句过长）
    batch_size = 500
    deleted_count = 0
    
    for i in range(0, missing_count, batch_size):
        batch_ids = missing_object_ids[i:i+batch_size]
        placeholders = ', '.join(['%s'] * len(batch_ids))
        
        cursor.execute(
            f"DELETE FROM artifact WHERE object_id IN ({placeholders})",
            batch_ids
        )
        deleted_count += cursor.rowcount
        print(f"[PROGRESS] 已删除 {deleted_count}/{missing_count} 条记录")
    
    return deleted_count

def main():
    parser = argparse.ArgumentParser(description='检查并删除artifact表中无效图片记录')
    parser.add_argument('--delete', action='store_true', 
                       help='执行删除操作（默认仅统计不删除）')
    parser.add_argument('--force', action='store_true',
                       help='跳过删除确认，直接执行删除（谨慎使用）')
    args = parser.parse_args()

    connection = None
    try:
        # 建立数据库连接
        connection = pymysql.connect(**DB_CONFIG)
        print("[INFO] 数据库连接成功")
        
        with connection.cursor() as cursor:
            # 第一步：检查文件存在性
            result = check_image_files(cursor)
            
            # 第二步：如果指定了--delete参数，执行删除操作
            if args.delete and result['missing'] > 0:
                if not args.force:
                    confirm = input("\n⚠️ 确认删除以上所有无效记录？(输入YES继续): ")
                    if confirm.strip() != 'YES':
                        print("[INFO] 用户取消删除操作")
                        return
                
                print("\n[INFO] 开始删除无效记录...")
                deleted_count = delete_missing_artifacts(cursor, result['missing_object_ids'])
                
                # 提交事务
                connection.commit()
                print(f"\n✅ 删除完成！共删除 {deleted_count} 条无效记录")
                
                # 验证删除结果
                cursor.execute("SELECT COUNT(*) as count FROM artifact")
                remaining = cursor.fetchone()['count']
                print(f"📊 删除后表中剩余记录数: {remaining}")
            elif args.delete and result['missing'] == 0:
                print("\n[INFO] 没有发现无效记录，无需删除")
            else:
                print("\n[INFO] 仅完成统计，未执行删除操作")
                print("[INFO] 如需删除，请添加 --delete 参数重新运行")
        
    except pymysql.MySQLError as e:
        if connection:
            connection.rollback()
        print(f"\n[ERROR] 数据库错误: {e}")
        print("[ERROR] 事务已回滚，没有数据被删除")
    except Exception as e:
        if connection:
            connection.rollback()
        print(f"\n[ERROR] 程序运行错误: {e}")
        print("[ERROR] 事务已回滚，没有数据被删除")
    finally:
        if connection and connection.open:
            connection.close()
            print("\n[INFO] 数据库连接已关闭")

if __name__ == "__main__":
    main()