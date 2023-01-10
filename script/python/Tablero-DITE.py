# %% Programa para la generación de tablero DITE
# PBI Referencia: https://app.powerbi.com/view?r=eyJrIjoiYzI3OTA0YzItZGU5Yi00MGEzLWE2ZjItODAyMzY4YTg4NDdhIiwidCI6IjE3OWJkZGE4LWQ5NjQtNDNmZi1hZDNiLTY3NDE4NmEyZmEyOCIsImMiOjR9&pageName=ReportSection
# Autor: Juan Carlos Tovar
# Fecha Creación: 10/11/2022
# Fecha Modificación: 11/12/2022
import csv
import time
from calendar import month
from turtledemo.chaos import h

from sqlalchemy import create_engine
import pandas as pd
import mariadb
from conn import Conn
from utils import SendEmail
import glob
from sqlalchemy import text
import os
import numpy as np
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
import warnings
import datetime
import time

warnings.filterwarnings("ignore")

# %% Variables
engine = Conn().dite_prod()

#%% Año en curso
anio = datetime.datetime.now()

# %% Rutas
path_base = r'D:\PROYECTOS\MINEDU\DATA\DITE\PJ02_PR01\INPUT'
path_reports = r'D:\PROYECTOS\MINEDU\SCRIPT\DITE\PJ02_PR01\sql'
path_export = r'D:\PROYECTOS\MINEDU\DATA\DITE\PJ02_PR01\OUTPUT'
path_cdr = path_base + r'\CDR' + '\\' + str(anio.year)
columns_csv = r'\COLUMNAS\cdr_columns.csv'
columns_report = path_base + r'\COLUMNAS\columns_report_vf.txt'
operadores_file = path_base + r'\NOMBRE DE OPERADORES.XLSX'
file_columns_ivr = path_base + r'\COLUMNAS\columns_report_ivr.txt'
file_columns_out = path_base + r'\COLUMNAS\columns_report_out.txt'
file_columns_in = path_base + r'\COLUMNAS\columns_report_in.txt'
all_files = glob.glob(path_reports + "/*.sql")


# %% Funciones


def read_sql_file(sql_file):
    with open(sql_file, 'r') as f:
        output = f.read()
    return output


def execute_sql_file(sql):
    print("Reading dataframe")
    try:
        df = pd.read_sql(text(sql), engine)
        print(df.shape)
    except Exception as e:
        print(e.args)

    return df


def export_data(df, path_export_i):
    print("Exporting")
    try:
        # df.to_csv(path_export_i, index=False, quoting=csv.QUOTE_NONNUMERIC)
        df.to_excel(path_export_i, index=False, encoding='utf8')
        print("Datos exportados satisfactoriamente")
        print("***********************************")
    except Exception as e:
        print(e.args)

    return df


def read_cdr(in_path_cdr, arr):
    c = 0
    final_df = pd.DataFrame(columns=arr)
    for base, dirs, files in os.walk(in_path_cdr):
        os.chdir(base)
        len_ = len([name for name in os.listdir(base) if os.path.isfile(os.path.join(base, name))])
        print(base + ': ' + str(len_) + ' files')
        if len_ > 0:
            for cdr in os.listdir(base):
                df_i = pd.read_csv(cdr)
                final_df = pd.concat([final_df, df_i], ignore_index=True)
        c = len_ + c

    return final_df


def transform_cdr(df_cdr, arr_report):
    df_cdr = df_cdr[(df_cdr['cdrRecordType'] != 'INTEGER')]

    df_cdr[['dateTimeOrigination', 'dateTimeConnect', 'dateTimeDisconnect']] = \
        df_cdr[['dateTimeOrigination', 'dateTimeConnect', 'dateTimeDisconnect']].astype(str).astype(int)

    df_cdr['dateTimeOrigination_date'] = np.where((df_cdr['dateTimeOrigination'] != 0) |
                                                  (~df_cdr['dateTimeOrigination'].isnull()),
                                                  (pd.to_datetime(df_cdr['dateTimeOrigination'], unit='s')
                                                   - pd.Timedelta('05:00:00')).dt.date,
                                                  (pd.to_datetime(df_cdr['dateTimeDisconnect'], unit='s')
                                                   - pd.Timedelta('05:00:00')).dt.date)

    df_cdr['dateTimeOrigination_hour'] = np.where((df_cdr['dateTimeOrigination'] != 0) |
                                                  (~df_cdr['dateTimeOrigination'].isnull()),
                                                  (pd.to_datetime(df_cdr['dateTimeOrigination'], unit='s')
                                                   - pd.Timedelta('05:00:00')).dt.time,
                                                  (pd.to_datetime(df_cdr['dateTimeDisconnect'], unit='s')
                                                   - pd.Timedelta('05:00:00')).dt.time)

    df_cdr['dateTimeConnect_date'] = np.where(df_cdr['dateTimeConnect'] == 0 |
                                              (df_cdr['dateTimeConnect'].isnull()),
                                              (pd.to_datetime(df_cdr['dateTimeDisconnect'], unit='s')
                                               - pd.Timedelta('05:00:00')).dt.date,
                                              (pd.to_datetime(df_cdr['dateTimeConnect'], unit='s')
                                               - pd.Timedelta('05:00:00')).dt.date)

    df_cdr['dateTimeConnect_hour'] = np.where(df_cdr['dateTimeConnect'] == 0 |
                                              (df_cdr['dateTimeConnect'].isnull()),
                                              (pd.to_datetime(df_cdr['dateTimeDisconnect'], unit='s')
                                               - pd.Timedelta('05:00:00')).dt.time,
                                              (pd.to_datetime(df_cdr['dateTimeConnect'], unit='s')
                                               - pd.Timedelta('05:00:00')).dt.time)

    df_cdr['dateTimeDisconnect_date'] = np.where(df_cdr['dateTimeDisconnect'] == 0 |
                                                 (df_cdr['dateTimeDisconnect'].isnull()), np.nan,
                                                 (pd.to_datetime(df_cdr['dateTimeDisconnect'], unit='s')
                                                  - pd.Timedelta('05:00:00')).dt.date)

    df_cdr['dateTimeDisconnect_hour'] = np.where(df_cdr['dateTimeDisconnect'] == 0 |
                                                 (df_cdr['dateTimeDisconnect'].isnull()), np.nan,
                                                 (pd.to_datetime(df_cdr['dateTimeDisconnect'], unit='s')
                                                  - pd.Timedelta('05:00:00')).dt.time)

    df_cdr['Disconnect_Connect'] = np.where((df_cdr['dateTimeConnect'] == 0) |
                                            (df_cdr['dateTimeConnect'].isnull()),
                                            np.timedelta64(0, 'D'),
                                            pd.to_datetime(df_cdr['dateTimeDisconnect'], unit='s') - pd.to_datetime(
                                                df_cdr['dateTimeConnect'], unit='s'))

    df_cdr['Disconnect_Connect_days'] = df_cdr['Disconnect_Connect'].dt.days

    df_cdr['Disconnect_Connect_time'] = np.where(
        (df_cdr['Disconnect_Connect_days'] > 500) | (df_cdr['Disconnect_Connect_days'] < 0), np.nan,
        df_cdr['Disconnect_Connect'].astype(str))

    df_cdr['Disconnect_Connect_time'] = df_cdr['Disconnect_Connect_time'].str[-8:]

    df_cdr['Disconnect_Connect_seconds'] = np.where(
        (df_cdr['Disconnect_Connect_days'] > 500) | (df_cdr['Disconnect_Connect_days'] < 0), np.nan,
        df_cdr['Disconnect_Connect'].dt.seconds)

    df_cdr['Dur_timbrado'] = np.where((df_cdr['dateTimeConnect'] == 0) |
                                      (df_cdr['dateTimeConnect'].isnull()),
                                      pd.to_datetime(df_cdr['dateTimeDisconnect'], unit='s') - pd.to_datetime(
                                          df_cdr['dateTimeOrigination'], unit='s'),
                                      pd.to_datetime(df_cdr['dateTimeConnect'], unit='s') - pd.to_datetime(
                                          df_cdr['dateTimeOrigination'], unit='s'))

    df_cdr['Dur_timbrado_days'] = df_cdr['Dur_timbrado'].dt.days
    df_cdr['Dur_timbrado_time'] = np.where((df_cdr['Dur_timbrado_days'] > 500) | (df_cdr['Dur_timbrado_days'] < 0),
                                           np.nan,
                                           df_cdr['Dur_timbrado'].astype(str))

    df_cdr['Dur_timbrado_time'] = df_cdr['Dur_timbrado_time'].str[-8:]

    df_cdr['Dur_timbrado_seconds'] = np.where(
        (df_cdr['Dur_timbrado_days'] > 500) | (df_cdr['Dur_timbrado_days'] < 0), np.nan,
        df_cdr['Dur_timbrado'].dt.seconds)

    df_cdr_report = df_cdr[arr_report]
    df_cdr_report['dateTimeOrigination_date'] = pd.to_datetime(df_cdr_report['dateTimeOrigination_date'],
                                                               format="%Y-%m-%d")

    return df_cdr_report


def get_cdr_ivr(df_cdr_report, file_columns_ivr):
    df_cdr_ivr = df_cdr_report[df_cdr_report['destDeviceName'].str.contains("P_EDU", na=False)]

    df_cdr_ivr = df_cdr_ivr[~df_cdr_ivr['dateTimeConnect_date'].isnull()]

    df_cdr_ivr['tipo_resp'] = np.where(df_cdr_ivr['Disconnect_Connect_seconds'] == 0, 'A', 'R')

    df_cdr_ivr['Telf_DIED'] = 'IVR'

    df_cdr_ivr = df_cdr_ivr[df_cdr_ivr['callingPartyNumber'].str.len() >= 7]

    df_cdr_ivr.sort_values(by=['dateTimeConnect_date', 'dateTimeConnect_hour'], inplace=True)

    arr_ivr = pd.read_csv(file_columns_ivr)

    df_cdr_ivr = df_cdr_ivr[list(arr_ivr['CDR_name'])]
    df_cdr_ivr.sort_values(by=['dateTimeConnect_date', 'dateTimeConnect_hour'], inplace=True)
    df_cdr_ivr.columns = list(arr_ivr['report_name'])
    df_cdr_ivr['Fecha'] = pd.to_datetime(df_cdr_ivr['Fecha'], format="%Y-%m-%d")
    df_cdr_ivr['Fecha'] = df_cdr_ivr['Fecha'].dt.strftime("%d/%m/%Y")

    return df_cdr_ivr


def get_cdr_out(df_cdr_report, file_columns_out, df_oper):
    df_cdr_out = df_cdr_report[df_cdr_report['origDeviceName'].isin(list(df_oper['NOMBRE JABBER']))]

    df_cdr_out = pd.merge(df_cdr_out, df_oper, left_on='origDeviceName', right_on='NOMBRE JABBER', how='left')

    df_cdr_out['tipo_resp'] = np.where(df_cdr_out['Disconnect_Connect_seconds'] == 0, 'A', 'R')

    df_cdr_out = df_cdr_out[~df_cdr_out['dateTimeConnect_date'].isnull()]

    df_cdr_out = df_cdr_out[df_cdr_out['tipo_resp'] == 'R']
    df_cdr_out = df_cdr_out[df_cdr_out['finalCalledPartyNumber'].str.len() >= 7]

    df_cdr_out["Global"] = np.where(df_cdr_out['finalCalledPartyNumberPartition'].str.contains("PTT_CELL_HQ"), "CEL",
                                    np.where(df_cdr_out['finalCalledPartyNumberPartition'].str.contains("PTT_LDN_HQ"),
                                             "DDN",
                                             np.where(
                                                 df_cdr_out['finalCalledPartyNumberPartition'].str.contains(
                                                     "PTT_LOCAL"),
                                                 "LOC", "NA")))

    arr_out = pd.read_csv(file_columns_out)

    df_cdr_out = df_cdr_out[list(arr_out['CDR_name'])]

    df_cdr_out.columns = list(arr_out['report_name'])
    df_cdr_out['Fecha'] = pd.to_datetime(df_cdr_out['Fecha'], format="%Y-%m-%d")
    df_cdr_out['Fecha'] = df_cdr_out['Fecha'].dt.strftime("%d/%m/%Y")

    return df_cdr_out


def get_cdr_in(df_cdr_report, file_columns_in, df_oper, df_anexo):
    df_cdr_in = df_cdr_report[df_cdr_report['destDeviceName'].isin(list(df_oper['NOMBRE JABBER']))]

    df_cdr_in = pd.merge(df_cdr_in, df_oper, left_on='destDeviceName', right_on='NOMBRE JABBER', how='left')

    df_cdr_in['tipo_resp'] = np.where(df_cdr_in['Disconnect_Connect_seconds'] == 0, 'A', 'R')

    df_cdr_in = df_cdr_in[~df_cdr_in['dateTimeConnect_date'].isnull()]

    df_cdr_in = df_cdr_in[~(df_cdr_in['Anexo'].isnull())]

    df_cdr_in = df_cdr_in[df_cdr_in['finalCalledPartyNumber'].isin(list(df_anexo['Anexo']))]
    df_cdr_in = df_cdr_in[df_cdr_in['originalCalledPartyNumber'].isin(list(df_anexo['Anexo']))]

    df_cdr_in["Global"] = np.where(df_cdr_in['finalCalledPartyNumberPartition'].str.contains("PTT_CELL_HQ"), "CEL",
                                   np.where(df_cdr_in['finalCalledPartyNumberPartition'].str.contains("PTT_LDN_HQ"),
                                            "DDN",
                                            np.where(
                                                df_cdr_in['finalCalledPartyNumberPartition'].str.contains("PTT_LOCAL"),
                                                "LOC", "NA")))

    df_cdr_in["Desv"] = np.where(df_cdr_in['originalCalledPartyNumber'] == '30990', 'CENTRAL P_EDU',
                                 np.where(df_cdr_in['originalCalledPartyNumber'] == '20001', 'CENTRAL 6155800', ''))

    df_cdr_in["Desv"] = np.where(df_cdr_in['tipo_resp'] == 'A', '', df_cdr_in['tipo_resp'])

    df_cdr_in.sort_values(by=['dateTimeConnect_date', 'dateTimeConnect_hour'], inplace=True)

    df_cdr_in = df_cdr_in[df_cdr_in['callingPartyNumber'].str.len() >= 7]

    arr_in = pd.read_csv(file_columns_in)

    df_cdr_in = df_cdr_in[list(arr_in['CDR_name'])]

    df_cdr_in.columns = list(arr_in['report_name'])
    df_cdr_in['Fecha'] = pd.to_datetime(df_cdr_in['Fecha'], format="%Y-%m-%d")
    df_cdr_in['Fecha'] = df_cdr_in['Fecha'].dt.strftime("%d/%m/%Y")

    return df_cdr_in


# %% Ejecución para GLPI
for file in all_files:
    i = file.split('\\')[-1]
    i = i.split('.')[0]
    path_export_i = path_export + "\\" + i + ".xlsx"
    print(i)
    glpi_sql = read_sql_file(file)
    df = execute_sql_file(glpi_sql)
    export_data(df, path_export_i)

# %% Ejecución completa de los CDR
"""
now = datetime.datetime.now()
cdr_csv = r'D:\2022\df_cdr_vf2.csv'
#cdr_csv = path_export + '\\' + 'df_cdr.csv'

arr = np.loadtxt(columns_csv, dtype=str)
#df_cdr = read_cdr(path_cdr, arr)

#df_cdr.to_csv(cdr_csv, index=False)

# En caso de cargar un reporte acumulado existente
df_cdr = pd.read_csv(cdr_csv)
# %%
arr_report = np.loadtxt(columns_report, dtype=str)
df_cdr_report = transform_cdr(df_cdr, arr_report)

# Lista de operadores
df_oper = pd.read_excel(operadores_file)
df_oper = df_oper[list(df_oper.columns[:7])]

# Datos IVR
df_cdr_ivr = get_cdr_ivr(df_cdr_report, file_columns_ivr)

# Datos de Llamadas Salientes
df_cdr_out = get_cdr_out(df_cdr_report, file_columns_out, df_oper)

# Datos de los anexos
anexos_glob = {'Anexo': [30890, 30990, 20001]}
df_anexo = df_oper[['Anexo']]
df_anexo = df_anexo.append(pd.DataFrame(anexos_glob))
df_anexo[['Anexo']] = df_anexo[['Anexo']].astype(str)

# Datos de Llamadas Entrantes
df_cdr_in = get_cdr_in(df_cdr_report, file_columns_in, df_oper, df_anexo)
# --
"""
#%% Exportamos Reporte CDR
"""
end = datetime.datetime.now()
with pd.ExcelWriter(path_export+'\\'+'reporte_tablero_llamadas.xlsx') as writer:
    df_cdr_in.to_excel(writer, sheet_name='LLAM ENTRANTES', index=False)
    df_cdr_out.to_excel(writer, sheet_name='LLAM SALIENTES', index=False)
    df_cdr_ivr.to_excel(writer, sheet_name='IVR', index=False)

duration = end - now
print(duration)
"""
#%% Envío de correo
print("Enviando correo")
file_name = r'Report_tickets_DITE.xlsx'
file_path = path_export + '\\' + file_name
obj_correo = SendEmail()
obj_correo.send_email(file_name, file_path, "[DITE-GLPI] - Reporte de Tickets")
print("Correo enviado")