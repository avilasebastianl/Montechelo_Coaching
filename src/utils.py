import os
import sys
import glob 
import time
import yaml
import json
import logging
import datetime
import numpy as np
import pandas as pd
import logging.config
from paths import *
from pandas import DataFrame
from datetime import timedelta
from urllib.parse import quote
from sqlalchemy import Table, MetaData, create_engine, Column,text,Engine
sys.path.append(path_to_config)
from credentials import *

with open(os.path.join(path_to_config,"logger.yml")) as f:
    logging.config.dictConfig(yaml.safe_load(f))

# * Creación de motores de MySQL
def mysql_engine(ip:str) -> Engine:
    """Creacion de motor de MySQL para generar conexiones y acceso a metadata dentro de una instancia determinada por su ip

    Args:
        ip (str): IP de instancia MySQL donde se creara conexion

    Returns:
        Engine: Motor de MySQL con capacidad de crear conexciones y dar acceso a metadata dependiente a su base de dadtos de conexion
    """    
    return create_engine(f'mysql+pymysql://{dict_user.get(ip)}:{quote(dict_pass.get(ip))}@{ip}:{dict_port.get(ip)}/{dict_bbdd.get(ip)}',pool_recycle=9600,isolation_level="AUTOCOMMIT")

# * Funcion para leer el archivo .json con las tablas a ejecutar
def data_to_run() -> json:
    """Lectura de archivo JSON con los elementos a ejecutar en proceso de ETL

    Returns:
        json: Cadena de texto tipo json con los valores a ejecutar
    """    
    with open(os.path.join(path_to_data,'data_to_run.json')) as file:
        return json.load(file)
    
# * Funcion para obtener el ultimo registro filtrando dentro de una tabla y columna especifica
def get_last_id_date(table_name:str, column_name:str, ip:str) -> str:
    """ Obtiene el ultimo registro (Maximo) almacenado dentro de una tabla especifica filtrando por la columna asignada 

    Args:
        table_name (str): Nombre de la tabla
        column_name (str): Columna asiganada para filtrar la información
        ip (str): IP de instancia de MySQl donde se revisara la tabla

    Returns:
        str: Ultimo registro dentro de la tabla, sea un tipo fecha hora o id
    """    
    try:
        engine_destino = mysql_engine(ip)
        with engine_destino.connect() as conn:
            sql = f"SELECT `{column_name}` FROM `tb_{table_name}` ORDER BY `{column_name}` DESC LIMIT 1;"
            logging.getLogger("user").debug(sql)
            df = pd.read_sql(sql,conn)
        if not df.empty:
            last_row = df.iloc[0,0] - timedelta(hours = 2) if column_name !='id' else df.iloc[0,0]
            return last_row
        else:
            logging.getLogger("user").debug(f"tb_{table_name}  vacia")
            last_row = 1 if column_name == 'id' else '2024-04-01 00:00:00'
            return last_row
    except Exception as e:
        logging.getLogger("dev").error(f"{table_name} -> {e}")
        last_row = 1 if column_name == 'id' else '2024-04-01 00:00:00'
        logging.getLogger("user").debug(f"Can't obtain maximum of tb_{table_name}. Standard: {last_row}")
        return last_row
    finally:    
        logging.getLogger("user").debug(f"Last row in {ip} -> tb_{table_name} >> {last_row}")

# * Funcion para obtener el dataframe
def get_data_sql(table_name:str, column_name:str, ip_des:str, ip_or:str, fecha_inicio:datetime, fecha_fin:datetime) -> DataFrame:
    """ Generacion de dataframe basado en los parametros según origen y destino.

    Args:
        table_name (str): Nombre de la tabla a consumir
        column_name (str): Nombre de la columna por la cual se filtrara la tabla
        ip_des (str): IP de instancia MySQL de donde se verifica el ultimo registro segun los anteriores paramentros
        ip_or (str): IP de instancia MySQL de donde se obtendra el DataFrame
        fecha_inicio (datetime, optional): Filtro de fecha para reeejecutar información. Defaults to None.
        fecha_fin (datetime, optional): Filtro de fecha para reeejecutar información. Defaults to None.

    Returns:
        DataFrame: Retorna DataFrame de pandas con las columna tipo time corregidas para insercion a MySQL
    """    
    logging.getLogger("user").debug(f"Ejecutando desde {fecha_inicio} hasta {fecha_fin} -> {table_name:30}")
    if ip_or != ip_montechelo:
        table_name = f'tb_{table_name}'
    sql_query = f"SELECT * FROM `{table_name}` WHERE `{column_name}` BETWEEN '{fecha_inicio}' AND '{fecha_fin}'"
    logging.getLogger("user").debug(sql_query)

    engine_or = mysql_engine(ip_or)
    with engine_or.connect() as conn:
        df = pd.read_sql(sql_query,conn)

    for i in list(df.select_dtypes(include=['timedelta64']).columns):
        df[i] = df[i].astype(str).map(lambda x: x[7:])
    logging.getLogger("user").debug(f"{table_name} with {df.shape[0]} rows")
    return df

# * Funcion para cargue a DW desde el origen de la informacion
def load_to_dw(table_name:str, column_name:str, fecha_inicio:datetime.datetime = None, fecha_fin:datetime.datetime = None) -> None:
    """Funcion para cargar informacion a DW desde el origen

    Args:
        table_name (str): Tabla a insertar
        column_name (str): Columna por filtrar
        fecha_inicio (datetime.datetime, optional): Fecha de reejecucion. Defaults to None.
        fecha_fin (datetime.datetime, optional): Fecha de reejecucion. Defaults to None.
    """    
    ini = time.time()
    try:
        if not fecha_inicio and not fecha_fin:
            fecha_inicio = get_last_id_date(table_name,column_name,ip_dw)
            fecha_fin = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") if column_name != 'id' else fecha_inicio + 300
        df = get_data_sql(table_name,column_name,ip_dw,ip_montechelo,fecha_inicio,fecha_fin)
        if not df.empty:
            engine_dw = mysql_engine(ip_dw)
            tabla_real = Table(f"tb_{table_name}", MetaData(), autoload_with = engine_dw)
            tabla_real.create(bind=engine_dw,checkfirst=True)
            columnas_nuevas = [Column(c.name, c.type) for c in tabla_real.c]
            tmp = Table(f"tb_{table_name}_tmp", MetaData(), *columnas_nuevas)
            tmp.drop(bind = engine_dw,checkfirst=True)
            tmp.create(bind = engine_dw)
            logging.getLogger("user").debug(f"Insertando datos en la temporal: tb_{table_name}_tmp")
            with engine_dw.connect() as conn_dw:
                df.to_sql(f"tb_{table_name}_tmp", conn_dw, if_exists='append',index=False,chunksize=100000)
                logging.getLogger("user").debug(f"Insertando datos en la tabla real: tb_{table_name}")
                conn_dw.execute(text(f"REPLACE INTO `{tabla_real.name}` SELECT * FROM `{tmp.name}`;"))
                logging.getLogger("user").info(f"[ SUCCESS DW     -> {tabla_real.name:^26} >> datetime range: ( {fecha_inicio} - {fecha_fin} ) >> {df.shape[0]:^6} rows >> {time.time() - ini:.2f} sec ]")
            tmp.drop(bind = engine_dw)
        else:
            logging.getLogger("user").info(f"[ DW -> Dataframe vacio de: {table_name} datetime range: ( {fecha_inicio} - {fecha_fin} ) ]")
    except Exception as e:
        logging.getLogger("dev").error(e)

# * Funcion para cargue a produccion desde DW
def load_to_production(table_name:str, column_name:str, fecha_inicio:datetime.datetime=None, fecha_fin:datetime.datetime=None) -> None:
    """Cargue de datos a producción mediante fecha inicio y fecha fin de ejecucion

     Args:
        table_name (str): Tabla a insertar
        column_name (str): Columna por filtrar
        fecha_inicio (datetime.datetime, optional): Fecha de reejecucion. Defaults to None.
        fecha_fin (datetime.datetime, optional): Fecha de reejecucion. Defaults to None.
    """    
    ini = time.time()
    try:
        if not fecha_inicio and not fecha_fin:
            fecha_inicio = get_last_id_date(table_name,column_name,ip_prod)
            fecha_fin = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") if column_name != 'id' else fecha_inicio + 300
        df = get_data_sql(table_name,column_name,ip_prod,ip_dw,fecha_inicio,fecha_fin)
        if not df.empty:
            engine_dw  = mysql_engine(ip_dw)
            engine_prod = mysql_engine(ip_prod)
            tabla_real = Table(f"tb_{table_name}", MetaData(), autoload_with = engine_dw)
            tabla_real.create(bind=engine_prod,checkfirst=True)
            columnas_nuevas = [Column(c.name, c.type) for c in tabla_real.c]
            tmp = Table(f"tb_{table_name}_tmp", MetaData(), *columnas_nuevas)
            tmp.drop(bind = engine_prod,checkfirst=True)
            tmp.create(bind = engine_prod)
            with engine_prod.connect() as conn_prod:
                logging.getLogger("user").debug(f"Insertando datos en produccion: tb_{table_name}")
                df.to_sql(f"tb_{table_name}_tmp", conn_prod, if_exists='append',index=False,chunksize=100000)
                conn_prod.execute(text(f"REPLACE INTO `{tabla_real.name}` SELECT * FROM `{tmp.name}`;"))
                logging.getLogger("user").info(f"[ SUCCESS DISTRO -> {tabla_real.name:^26} >> datetime range: ( {fecha_inicio} - {fecha_fin} ) >> {df.shape[0]:^6} rows >> {time.time() - ini:.2f} sec ]")
            tmp.drop(bind = engine_prod)
        else:
            logging.getLogger("user").info(f"[ Distro -> Dataframe vacio de: tb_{table_name} datetime range: ( {fecha_inicio} - {fecha_fin} ) ]")
    except Exception as e:
        logging.getLogger("dev").error(e)

# * Funcion para listar tablas con su respectivo cid 
def list_cid_tables():
    print(f"[      Table                |   CID ]")
    print(f"[-----------------------------------]")
    with open(os.path.join(path_to_data,'data_to_run.json')) as file:
        for i in json.load(file):
            print(f"[ {i['table_name']:25} | {i['cid']:5} ]")

# * Funcion de ejecucion a dw
def load_dw(cid:int = None):
    fecha_inicio = str(sys.argv[2])+" "+str(sys.argv[3]) if len(sys.argv) > 3 else None
    fecha_fin    = str(sys.argv[4])+" "+str(sys.argv[5]) if len(sys.argv) > 3 else None
    [ load_to_dw(i["table_name"],i["column_name"],fecha_inicio if fecha_inicio else None,fecha_fin if fecha_fin else None) for i in data_to_run() ]

# * Funcion de ejecucion de distro
def load_distro(cid:int = None):
    fecha_inicio = str(sys.argv[2])+" "+str(sys.argv[3]) if len(sys.argv) > 3 else None
    fecha_fin    = str(sys.argv[4])+" "+str(sys.argv[5]) if len(sys.argv) > 3 else None
    [ load_to_production(i["table_name"],i["column_name"],fecha_inicio if fecha_inicio else None,fecha_fin if fecha_fin else None) for i in data_to_run() ]

# * Funcion de ejecucion mediante cid
def exec_by_cid():
    cid = int(sys.argv[2]) if len(sys.argv) > 2 else None
    fecha_inicio = sys.argv[3] + " " + sys.argv[4] if len(sys.argv) > 4 else None
    fecha_fin = sys.argv[5] + " " + sys.argv[6] if len(sys.argv) > 6 else None
    for i in data_to_run():
        if i["cid"] == cid:
            load_to_dw(i["table_name"], i["column_name"], fecha_inicio, fecha_fin)
            load_to_production(i["table_name"], i["column_name"], fecha_inicio, fecha_fin)

# * Funcion de mostrar ayuda
def show_help():
    with open(os.path.join(path_to_docs,"documentation.txt"),'r') as file:
        print(file.read())

dict_actions = {
    '--help': show_help,
    '-h': show_help,
    '--list': list_cid_tables, # TODO: list
    '-l': list_cid_tables, # TODO: list
    '--matriz': load_dw, # TODO: dw
    '-m': load_dw, # TODO: dw
    '--distro': load_distro, # TODO: distro
    '-d': load_distro, # TODO: distro
    '--cid':exec_by_cid, # TODO: cid
    '-c':exec_by_cid, # TODO: cid
    '-exe': [load_dw, load_distro], # TODO: execute
    '--execute': [load_dw, load_distro] # TODO: execute
}
