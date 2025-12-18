import json
import pymysql
from datetime import datetime
import os


def clean_flight_data(flight):
    """清洗单条航班数据，处理空值和异常格式"""
    cleaned = {}

    # 处理出发地和目的地
    cleaned['departure'] = flight.get('出发地', '').strip() or None
    cleaned['destination'] = flight.get('目的地', '').strip() or None

    # 处理航班类型
    cleaned['flight_type'] = "国内航班"

    # 处理航班日期（确保格式正确）
    try:
        date_str = flight.get('航班日期', '').strip()
        if date_str:
            datetime.strptime(date_str, '%Y-%m-%d')
            cleaned['flight_date'] = date_str
        else:
            cleaned['flight_date'] = None
    except ValueError:
        cleaned['flight_date'] = None

    # 处理航班代码（严格非空校验）
    flight_code = flight.get('航班代码', '').strip()
    cleaned['flight_code'] = flight_code if flight_code else None

    # 处理价格（严格非空且可转换为数字）
    price_str = flight.get('价格', '').strip()
    if not price_str or price_str in ['无价格', '暂无', 'NA', 'N/A']:
        cleaned['price'] = None
    else:
        try:
            price_str = price_str.replace('¥', '').replace(',', '').replace(' ', '')
            cleaned['price'] = int(price_str)
        except ValueError:
            cleaned['price'] = None

    return cleaned


def create_test_table_if_not_exists(conn):
    """在已连接的数据库中创建test表（如果不存在）"""
    cursor = conn.cursor()
    # 根据实际数据库类型调整SQL语法（如日期类型在PostgreSQL中为DATE）
    cursor.execute('DROP TABLE IF EXISTS ticket_info')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS ticket_info (
        id INT PRIMARY KEY AUTO_INCREMENT,
        departure VARCHAR(100) NOT NULL,
        destination VARCHAR(100) NOT NULL,
        flight_type VARCHAR(50),
        flight_date DATE NOT NULL,
        flight_code VARCHAR(50) NOT NULL,
        price double NOT NULL,
        source_file VARCHAR(255) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ''')
    conn.commit()
    print("已确保test表存在（若不存在则已创建）")


def insert_flights_to_test_table(conn, flights, source_file):
    """将清洗后的航班数据插入到test表，过滤无效记录"""
    cursor = conn.cursor()
    insert_sql = '''
    INSERT INTO ticket_info (
        departure, destination, flight_type, 
        flight_date, flight_code, price, source_file
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    '''  # MySQL使用%s作为占位符，PostgreSQL使用%s，SQL Server使用?

    # 过滤条件：确保航班代码、价格及关键信息有效
    data_to_insert = [
        (
            flight['departure'],
            flight['destination'],
            flight['flight_type'],
            flight['flight_date'],
            flight['flight_code'],
            flight['price'],
            source_file
        ) for flight in flights
        if (flight['flight_code'] is not None and
            flight['price'] is not None and
            flight['departure'] is not None and
            flight['destination'] is not None and
            flight['flight_date'] is not None)
    ]

    if data_to_insert:
        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()
        print(f"从 {source_file} 成功插入 {cursor.rowcount} 条数据到test表")
    else:
        print(f"{source_file} 中没有符合条件的数据可插入到test表")


def process_single_file(file_path, conn):
    """处理单个JSON文件并插入数据到数据库"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                print(f"错误：{file_path} 格式不正确，跳过该文件")
                return

        # 提取航班列表（兼容数组或包含"航班列表"键的对象）
        if isinstance(data, list):
            flight_list = data
        else:
            flight_list = data.get('航班列表', [])

        if not flight_list:
            print(f"{file_path} 中未找到航班数据")
            return

        # 清洗并插入数据
        cleaned_flights = [clean_flight_data(flight) for flight in flight_list]
        # 传入完整文件路径（便于定位文件位置），也可保留basename
        insert_flights_to_test_table(conn, cleaned_flights, os.path.abspath(file_path))

    except Exception as e:
        conn.rollback()  # 出错时回滚事务
        print(f"处理 {file_path} 时出错：{str(e)}")


def connect_to_database():
    """连接到已存在的airTicketPriceForecast数据库，需手动填写连接信息"""
    try:
        # 请根据实际数据库信息修改以下参数
        conn = pymysql.connect(
            host='localhost',  # 数据库主机地址
            port=3308,  # 数据库端口（MySQL默认3306）
            user='root',  # 数据库用户名
            password='123456',  # 数据库密码
            db='airTicketPriceForecast',  # 数据库名称（必须已存在）
            charset='utf8mb4'  # 显式指定字符集，避免中文乱码
        )
        print("成功连接到airTicketPriceForecast数据库")
        return conn
    except Exception as e:
        print(f"数据库连接失败：{str(e)}")
        return None


def process_all_json_files(root_folder, conn):
    """递归遍历根文件夹下的所有子文件夹，处理所有JSON文件"""
    # 遍历根文件夹下的所有文件和子文件夹
    for root, dirs, files in os.walk(root_folder):
        for filename in files:
            # 只处理JSON文件
            if filename.lower().endswith('.json'):  # 兼容大写.JSON后缀
                file_path = os.path.join(root, filename)
                print(f"\n开始处理：{file_path}")
                process_single_file(file_path, conn)


def process_flight_folders(root_folder):
    """处理指定根文件夹下的所有子文件夹中的JSON文件，插入到已存在的数据库"""
    # 连接到已存在的数据库
    conn = connect_to_database()
    if not conn:
        return

    try:
        # 创建test表（如果不存在）
        create_test_table_if_not_exists(conn)

        # 递归处理所有子文件夹中的JSON文件
        process_all_json_files(root_folder, conn)

        print("\n所有文件夹的JSON文件处理完成")

    except Exception as e:
        print(f"处理过程出错：{str(e)}")
    finally:
        if conn:
            conn.close()
            print("数据库连接已关闭")


if __name__ == "__main__":
    # JSON文件的根文件夹路径（所有子文件夹都在这个目录下）
    # 示例：如果有 json/北京/、json/上海/ 等子文件夹，填 json 即可
    root_json_folder = "json"

    if not os.path.exists(root_json_folder):
        print(f"错误：根文件夹 {root_json_folder} 不存在")
    else:
        process_flight_folders(root_json_folder)