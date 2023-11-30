import mysql.connector
from dateutil import tz
from datetime import datetime

import pandas as pd

class DatabaseContabilGaulke:
    def __init__(self, username, password, host, database):
        self.username = username
        self.password = password
        self.host = host
        self.database = database
    # ----
    def connection_database(self):
        try:
            config = {
                'user': self.username,
                'password': self.password,
                'host': self.host,
                'database': self.database,
            }

            conn = mysql.connector.connect(**config)
            return conn
        except Exception as e:
            print(f" ### ERROR CONNECTION DATABASE | ERROR: {e}")
            return None
    # ----
    def close_connection(self, object):
        try:
            object.close()
            print(f" >> DISCONNECT SUCCESS ")
            return True
        except:
            print(f" >> DISCONNECT ERROR ")
            return False
    # ----
    def create_dataframe_all_companies(self, query):
        dict_to_dataframe = {
            "id": list(),
            "cnpj": list(),
            "razao_social": list(),
            "id_acessorias": list(),
            "regime": list(),
        }
        try:
            for data in query:
                dict_to_dataframe["id"].append(             data[0] )
                dict_to_dataframe["cnpj"].append(           data[1] )
                dict_to_dataframe["razao_social"].append(   data[2] )
                dict_to_dataframe["id_acessorias"].append(  data[3] )
                dict_to_dataframe["regime"].append(         data[4] )

            df = pd.DataFrame.from_dict(dict_to_dataframe)
            return df
        except Exception as e:
            print(f"\n\n ### ERROR CONVETER QUERY TO DATAFRAME ALL COMPANIES | ERROR: {e}")
            return None
    # ----
    def create_dataframe_all_balancetes(self, query):
        dict_to_dataframe = {
            "id": list(),
            "obrigacao": list(),
            "cnpj": list(),
            "competencia": list(),
            "data_da_entrega": list(),
        }
        try:
            for data in query:
                dict_to_dataframe["id"].append(                 data[0] )
                dict_to_dataframe["obrigacao"].append(          data[1] )
                dict_to_dataframe["cnpj"].append(               data[2] )
                dict_to_dataframe["competencia"].append(        data[3] )
                dict_to_dataframe["data_da_entrega"].append(    data[4] )


            df = pd.DataFrame.from_dict(dict_to_dataframe)
            return df
        except Exception as e:
            print(f"\n\n ### ERROR CONVETER QUERY TO DATAFRAME ALL BALANCETES | ERROR: {e}")
            return None
    # ----
    def create_dataframe_base_to_sheets(self, df_all_companies, df_all_balancetes):
        df_all_companies["data_entrega_lancado"] = "-"
        df_all_companies["competencia_lancado"] = "-"
        df_all_companies["data_entrega_mensal"] = "-"
        df_all_companies["competencia_mensal"] = "-"

        df_all_balancetes["data_da_entrega"] = df_all_balancetes["data_da_entrega"].dt.strftime('%d/%m/%Y')

        print(f"\n\n --------------- DF TO GOOGLE SHEETS ---------------  ")
        df_all_balancetes = df_all_balancetes.sort_values(by=["cnpj", "data_da_entrega", "obrigacao"])
        df_all_balancetes.drop_duplicates(subset=["obrigacao", "cnpj"], keep="last", inplace=True)

        print(df_all_balancetes)

        for i in df_all_companies.index:

            dt_lancado = None
            df_mensal = None
            cnpj = df_all_companies["cnpj"][i]
            
            query_balancete_lancado = df_all_balancetes[ (df_all_balancetes["cnpj"] == cnpj) & (df_all_balancetes["obrigacao"] == "BALANCETE LANÇADO") ]
            query_balancete_mensal = df_all_balancetes[ (df_all_balancetes["cnpj"] == cnpj)  & (df_all_balancetes["obrigacao"] == "BALANCETE MENSAL" ) ]
            
            
            try:
                dt_lancado = query_balancete_lancado["data_da_entrega"].values[0]
                df_all_companies["data_entrega_lancado"][i] = dt_lancado

                try:

                    competencia = query_balancete_lancado["competencia"].values[0]
                    df_all_companies["competencia_lancado"][i] = competencia
                    
                except:
                    pass
            except:
                pass

            # ----

            try:

                df_mensal = query_balancete_mensal["data_da_entrega"].values[0]
                df_all_companies["data_entrega_mensal"][i] = df_mensal

                try:
                    competencia = query_balancete_mensal["competencia"].values[0]
                    df_all_companies["competencia_mensal"][i] = competencia
                except:
                    pass

            except:
                pass
            
            
            print("\n\n -------------------------------------------- ")
            print(f"----> CNPJ: {cnpj} | dt_lancado: {dt_lancado} | df_mensal: {df_mensal}")

            print("\n\n ------------------------ DATA INFO ------------------------ ")
            print(query_balancete_lancado.info())
            print("\n\n -------------------------------------------- ")
            print(query_balancete_mensal.info())

        df_all_companies.to_excel("all_companies.xlsx")
        return df_all_companies
    # ----
    def get_all_data_google_sheets_periodo_contabil(self):
        pass
    # ----
    def get_all_data(self, table_name_companies, table_name_balancetes):
        dict_data = dict()
        conn = None
        cursor = None

        try:
            conn = self.connection_database()
            cursor = conn.cursor()

            print(f"\n #### INFO CONNECTION DATABASE | is_connected: {conn.is_connected()} | db: {conn.database} | connection_id: {conn.connection_id} ### \n")

            if conn is not None:
                comannd_query_all_companies = f"""SELECT id, cnpj, razao_social, id_acessorias, regime  FROM {table_name_companies};"""
                comannd_query_all_balancetes = f"""SELECT id, obrigacao, cnpj, competencia, data_da_entrega  FROM {table_name_balancetes};"""

                cursor.execute(comannd_query_all_companies)
                query_all_companies = cursor.fetchall()
                # ----
                cursor.execute(comannd_query_all_balancetes)
                query_all_balancetes = cursor.fetchall()


                print(comannd_query_all_companies)
                print(comannd_query_all_balancetes)

                tt_companies = len(query_all_companies)
                tt_balancetes = len(query_all_balancetes)
                print(f"""
                    ---------- RESUME QUERY ----------
                    --> tt_companies:  {tt_companies}
                    --> tt_balancetes: {tt_balancetes}
                """)

                df_all_companies = self.create_dataframe_all_companies(query=query_all_companies)
                df_all_balancetes = self.create_dataframe_all_balancetes(query=query_all_balancetes)

                print(f"\n\n ----------- DATAFRAME ALL COMPANIES ----------- ")
                print(df_all_companies)
                print(f"\n\n ----------- DATAFRAME ALL BALANCETES ----------- ")
                print(df_all_balancetes)
                
                df_to_sheets = self.create_dataframe_base_to_sheets(df_all_companies=df_all_companies, df_all_balancetes=df_all_balancetes)
                print(df_to_sheets)
                # print(f"\n ---> TT REGITER: {tt_register}")
                self.close_connection(object=conn)
                self.close_connection(object=cursor)
                return df_to_sheets
        except Exception as e:
            print(f" ### ERROR QUERY ALL DATABASE | ERROR: {e}")
            
        try:
            self.close_connection(object=conn)
            self.close_connection(object=cursor)
        except:
            pass
        
        return None
    # ----
