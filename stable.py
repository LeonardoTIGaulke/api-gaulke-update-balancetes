from config import DB_USERNAME, DB_PASSWORD, DB_DATABASE, DB_HOST, DB_TABLE_NAME_ALL_COMPANIES, DB_TABLE_NAME_ALL_BALANCETES
from config import SPREADSHEET_ID, SCOPES

from api.db.database import DatabaseContabilGaulke
from api.automate.api_google_sheets import API_GoogleSheets

def get_all_data_to_google_sheets():
    try:
        dataframe_all_companies = DatabaseContabilGaulke(
            username=DB_USERNAME, 
            password=DB_PASSWORD, 
            host=DB_HOST, 
            database=DB_DATABASE
            ).get_all_data(table_name_companies=DB_TABLE_NAME_ALL_COMPANIES, table_name_balancetes=DB_TABLE_NAME_ALL_BALANCETES)

        print(" -------------- DATA TO SHEETS -------------- ")
        print(dataframe_all_companies)
        
        APP_GOOGLE_SHEETS = API_GoogleSheets(scopes=SCOPES)
        APP_GOOGLE_SHEETS.update_data_sheets(dataframe_all_companies=dataframe_all_companies, sheet_name="Geral", spread_sheet_id=SPREADSHEET_ID)
        return 200
    except:
        return 400



if __name__ == "__main__":
    get_all_data_to_google_sheets()

    # import pandas as pd
    # dataframe_all_companies = pd.read_excel("all_companies.xlsx")

    # APP_GOOGLE_SHEETS = API_GoogleSheets(scopes=SCOPES)
    # APP_GOOGLE_SHEETS.update_data_sheets(dataframe_all_companies=dataframe_all_companies, sheet_name="Geral", spread_sheet_id=SPREADSHEET_ID)