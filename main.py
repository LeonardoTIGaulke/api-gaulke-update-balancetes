from config import DB_USERNAME, DB_PASSWORD, DB_DATABASE, DB_HOST, DB_TABLE_NAME_ALL_COMPANIES, DB_TABLE_NAME_ALL_BALANCETES

from api.db.database import DatabaseContabilGaulke

query = DatabaseContabilGaulke(
    username=DB_USERNAME, 
    password=DB_PASSWORD, 
    host=DB_HOST, 
    database=DB_DATABASE
    ).get_all_data(table_name_companies=DB_TABLE_NAME_ALL_COMPANIES, table_name_balancetes=DB_TABLE_NAME_ALL_BALANCETES)

# query = APP_DB.get_all_companies(table_name=DB_TABLE_NAME_ALL_BALANCETES)
print(query)

