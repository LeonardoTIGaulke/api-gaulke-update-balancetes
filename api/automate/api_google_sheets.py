from config_sheets import SPREADSHEET_ID, SCOPES, BASE_CONFIG_SHEETS, BASE_FISCAL, BASE_CONTABIL, BASE_RH

import os.path
from time import sleep

import pandas as pd

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build


class API_GoogleSheets:
    def __init__(self, scopes) -> None:
        self.scopes = scopes
        self.quota_process = 1


    def sleep_process(self, sleep_time):
        if self.quota_process >= 55:
            print(f"\n\n --- Aguardando periodo de {sleep_time} segundos para repor cotas da API Google Sheets.")
            print(f"\n\n --- Consumo de quotas no processo: {self.quota_process}")
            self.quota_process = 1
            return sleep(sleep_time)

    def get_creds(self):

        creds = None
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
        
        return creds

    
    def copy_paste_validation_data_fiscal(self, creds, data_update, sheet_name, spread_sheet_id):
        
        # Create connection Google Sheets API
        # creds = self.get_creds()
        service = build('sheets', 'v4', credentials=creds)

        RANGE_NAME = f'{sheet_name}!M2:N'
        print("------>>>>>>>>>>>>>>>>>> ", RANGE_NAME)
        tt_registros = len(data_update)
        print(">>> total de valores base  ", tt_registros)

        
        processPaste = ["PASTE_FORMULA"] #"PASTE_DATA_VALIDATION"] #, "PASTE_FORMULA"]
        INDEX_BASE_SHEET = 345342732
        for process in processPaste:
            print(f" \n\n ------------------------------  {process}")
            if process == "PASTE_FORMULA":
                
                # ----------------------- COPY/PASTE PERIODO
                data_source = {
                    "sheetId": INDEX_BASE_SHEET,
                    "startRowIndex": 1,
                    "endRowIndex": 2,
                    "startColumnIndex": 12,
                    "endColumnIndex": 13,
                }
                data_destination = {
                    "sheetId": INDEX_BASE_SHEET,
                    "startRowIndex": 2,
                    "endRowIndex": tt_registros+1,

                    "startColumnIndex": 12,
                    "endColumnIndex": 13,
                }
                # ----------------------- COPY/PASTE MESES EM ATRASO
                data_source_meses_atraso = {
                    "sheetId": INDEX_BASE_SHEET,
                    "startRowIndex": 1,
                    "endRowIndex": 2,
                    "startColumnIndex": 14,
                    "endColumnIndex": 15,
                }
                data_destination_meses_atraso = {
                    "sheetId": INDEX_BASE_SHEET,
                    "startRowIndex": 2,
                    "endRowIndex": tt_registros+1,

                    "startColumnIndex": 14,
                    "endColumnIndex": 15,
                }
            DATA = {
                "requests": [
                    {
                        "copyPaste": {
                            "source": data_source,
                            "destination": data_destination,
                            "pasteType": process,
                        }
                    }
                ]
            }
            DATA_MESES_ATRASO = {
                "requests": [
                    {
                        "copyPaste": {
                            "source": data_source_meses_atraso,
                            "destination": data_destination_meses_atraso,
                            "pasteType": process,
                        }
                    }
                ]
            }

            service.spreadsheets().batchUpdate(
                spreadsheetId=spread_sheet_id,
                body=DATA,
            ).execute()
            service.spreadsheets().batchUpdate(
                spreadsheetId=spread_sheet_id,
                body=DATA_MESES_ATRASO,
            ).execute()

            print(DATA)
        return self.quota_process



    def update_data_sheets(self, dataframe_all_companies, sheet_name, spread_sheet_id):
        
        creds = self.get_creds()
        # Cria uma conexão com a API do Google Sheets
        service = build('sheets', 'v4', credentials=creds)
        RANGE_NAME = f'{sheet_name}!A2:P'
        print("------>>>>>>>>>>>>>>>>>> ", RANGE_NAME)
        # Obtém os valores da coluna A
        result = service.spreadsheets().values().get(spreadsheetId=spread_sheet_id, range=RANGE_NAME).execute()
        data_sheet = result.get('values', [])
        print(data_sheet)

        dict_aux = dict()
        for data in data_sheet:
            print(data)
            value = None
            try:
                value = {
                    data[2]: data[15]
                }
            except:
                value = {
                    data[2]: "-"
                }
            dict_aux.update(value)

        list_errors = list()
        cont = 2
        dataframe_all_companies.sort_values(by="razao_social", inplace=True)
        values = list()
        values_date = list()
        values_formulas_01 = list()
        values_formulas_02 = list()

        for i in dataframe_all_companies.index:

            values.append(
                [
                    dataframe_all_companies["razao_social"][i],
                    int(dataframe_all_companies["id_acessorias"][i]),
                    dataframe_all_companies["cnpj"][i],
                    str(dataframe_all_companies["regime"][i]),
                ]
            )
            values_date.append(
                [
                    dataframe_all_companies["data_entrega_lancado"][i],
                    dataframe_all_companies["competencia_lancado"][i],
                    dataframe_all_companies["data_entrega_mensal"][i],
                    dataframe_all_companies["competencia_mensal"][i],
                ]
            )

            values_formulas_01.append(
                [
                    f"=ÍNDICE('Atual 07-11'!E:E;CORRESP(C{cont};'Atual 07-11'!C:C;0))",
                    f"=ÍNDICE('Atual 07-11'!F:F;CORRESP(C{cont};'Atual 07-11'!C:C;0))",
                    f"=ÍNDICE('Atual 07-11'!G:G;CORRESP(C{cont};'Atual 07-11'!C:C;0))",
                    f"""=SE(ÍNDICE('Acesso Bancário'!C:C;CORRESP(B{cont};'Acesso Bancário'!A:A;0))="SIM";"SIM";"NÃO")""",
                    # =SE(ÍNDICE('Acesso Bancário'!C:C;CORRESP(B2;'Acesso Bancário'!A:A;0))="SIM";"SIM";"NÃO")
                ]
            )
            values_formulas_02.append(
                [
                    dict_aux.get(dataframe_all_companies["cnpj"][i]),
                ]
            )



            cont += 1

        range_name = f'{sheet_name}!A2:D'
        body = {'values': values}
        result = service.spreadsheets().values().update(
            spreadsheetId=spread_sheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        # ---------------------------------------------
        range_name = f'{sheet_name}!I2:L'
        body = {'values': values_date}
        result = service.spreadsheets().values().update(
            spreadsheetId=spread_sheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        # ---------------------------------------------
        range_name = f'{sheet_name}!E2:H'
        body = {'values': values_formulas_01}
        result = service.spreadsheets().values().update(
            spreadsheetId=spread_sheet_id,
            range=range_name,
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()
        # ---------------------------------------------
        range_name = f'{sheet_name}!P2:P'
        body = {'values': values_formulas_02}
        result = service.spreadsheets().values().update(
            spreadsheetId=spread_sheet_id,
            range=range_name,
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()
        

        print(dataframe_all_companies)
        print(dataframe_all_companies.info())
        # ---------------------------------------------
        data_update = dataframe_all_companies["data_entrega_lancado"]
        self.copy_paste_validation_data_fiscal(creds=creds, data_update=data_update, sheet_name=sheet_name, spread_sheet_id=spread_sheet_id)

        

        self.quota_process += 1
        return True

        