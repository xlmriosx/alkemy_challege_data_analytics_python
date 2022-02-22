import requests
import pandas as pd
from datetime import datetime
import os
import csv
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import re

now = datetime.now()
current_date = now.strftime("%Y-%B")


def get_status_request(dbs):
    # verify state of request
    for db in dbs:
        print(f'[+] STATUS CODE: {db.status_code} for Request GET from DB {get_name_url(db)} ')
    print()


def get_name_url(db):
    url = list(db.url)
    url = url[0:len(url) - 4]

    categoria = ''
    for letra in reversed(url):
        if letra == '/':
            break
        categoria += letra

    categoria = categoria[::-1]
    return categoria


def conection_to_postgres():
    conection = psycopg2.connect(user='postgres', password='root')
    conection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return conection


def conection_to_db():
    conection = psycopg2.connect(database='datos_argentina', user='postgres', password='root')
    conection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return conection


def control_table(name_table, tables):
    if (name_table,) in tables:
        print(f'[+] Table "{name_table}" exists on "datos_argentina".')
    else:
        print(f'[-] Table "{name_table}" not exists on "datos_argentina".')
        if name_table == 'normal_biblioteca_cine_museo':
            cursor.execute(f'''
            CREATE TABLE normal_biblioteca_cine_museo(
            cod_localidad INT,
            id_provincia INT,
            id_departamento INT,
            categoria VARCHAR(40),
            provincia VARCHAR(60),
            localidad VARCHAR(40),
            nombre VARCHAR(120),
            domicilio VARCHAR(100),
            codigo_postal VARCHAR(10),
            numero_de_telefono VARCHAR(30),
            mail VARCHAR(80),
            web VARCHAR(120),
            fecha_carga DATE NOT NULL,
            PRIMARY KEY(cod_localidad,id_provincia,id_departamento,categoria,provincia,localidad,nombre,domicilio,
            codigo_postal,numero_de_telefono,mail,web));
            ''')
        elif name_table == 'estadistica_categoria':
            cursor.execute(f'''
            CREATE TABLE estadistica_categoria(
            fecha_carga DATE NOT NULL,
            categoria VARCHAR(40) NOT NULL,
            cant_registros INT NOT NULL,
            PRIMARY KEY(fecha_carga, categoria));''')
        elif name_table == 'estadistica_cine':
            cursor.execute(f'''
            CREATE TABLE estadistica_cine(
            fecha_carga DATE NOT NULL,
            provincia VARCHAR(40) NOT NULL,
            cant_butacas INT NOT NULL,
            cant_pantallas INT NOT NULL,
            cant_espacios_inca INT NOT NULL,
            PRIMARY KEY(fecha_carga, provincia));''')
        print(f'[+] Table "{name_table}" was create it on "datos_argentina".')
        print()


def control_null_incaa(dato):
    if dato == 's/d' or dato == '' or str(dato) == 'nan':
        return 0
    elif dato.lower() == 'si':
        return 1
    else:
        return dato


def control_null(dato):
    if dato == 's/d' or dato == '' or str(dato) == 'nan':
        return 0
    else:
        return dato


def insert_normal_table(
        cod_localidad, id_provincia, id_departamento, categoria, provincia, localidad,
        nombre, domicilio, codigo_postal, numero_de_telefono, mail, web, database):
    exception_exists, exception_undifined, new_rows = 0, 0, 0

    for rw in range(0, len(cod_localidad)):
        try:
            cursor.execute(f'''INSERT INTO normal_biblioteca_cine_museo(cod_localidad, id_provincia, id_departamento, categoria, provincia, localidad,
            nombre, domicilio, codigo_postal, numero_de_telefono, mail, web, fecha_carga)
            VALUES({control_null(cod_localidad[rw])},{control_null(id_provincia[rw])},
            {control_null(id_departamento[rw])},'{control_null(categoria[rw])}','{control_null(provincia[rw])}',
            '{control_null(re.sub(r"[']", " ", str(localidad[rw])))}',
            '{control_null(re.sub(r"[']", " ", str(nombre[rw])))}',
            '{control_null(re.sub(r"[']", " ", str(domicilio[rw])))}','{control_null(codigo_postal[rw])}',
            '{control_null(numero_de_telefono[rw])}','{control_null(mail[rw])}','{control_null(web[rw])}',
            '{now.strftime("%Y-%m-%d")}');''')
            new_rows += 1
        except psycopg2.errors.UniqueViolation:
            exception_exists += 1
        except:
            exception_undifined += 1

    print(f'[+] Was inserted on "normal_biblioteca_cine_museo" {new_rows} new rows from data base {database}.')
    print(f'[+] Was not inserted on "normal_biblioteca_cine_museo" {exception_exists} rows because now exists.')
    print(f'[-] Was not inserted on "normal_biblioteca_cine_museo" {exception_undifined} rows because apper an error.')
    print()


def assign_normal_table(name_csv, df):
    cod_localidad = tuple(df["Cod_Loc"].values)
    id_provincia = tuple(df["IdProvincia"].values)
    id_departamento = tuple(df["IdDepartamento"].values)
    mail = tuple(df["Mail"].values)
    codigo_postal = tuple(df["CP"].values)
    web = tuple(df["Web"].values)

    if name_csv == 'museo':
        categoria = tuple(df["categoria"].values)
        provincia = tuple(df["provincia"].values)
        localidad = tuple(df["localidad"].values)
        nombre = tuple(df["nombre"].values)
        numero_de_telefono = tuple(df["telefono"].values)
    else:
        categoria = tuple(df["Categoría"].values)
        provincia = tuple(df["Provincia"].values)
        localidad = tuple(df["Localidad"].values)
        nombre = tuple(df["Nombre"].values)
        numero_de_telefono = tuple(df["Teléfono"].values)

    if name_csv == 'museo':
        domicilio = tuple(df["direccion"].values)
    elif name_csv == 'cine':
        domicilio = tuple(df["Dirección"].values)
    elif name_csv == 'biblioteca_popular':
        domicilio = tuple(df["Domicilio"].values)

    return cod_localidad, id_provincia, id_departamento, categoria, provincia, localidad, nombre, domicilio, \
           codigo_postal, numero_de_telefono, mail, web


def configuration_env():
    print('[+] Activating virtual env...')
    os.system('pip install virtualenv')
    os.system('virtualenv venv')

    if input('Put W if you using Windows: ').lower() == 'w':
        os.system('cd venv/Scripts')
        os.system('activate')
    else:
        os.system('source venv/bin/activate')

    print('[+] Virtual env activated.')
    print('[+] Installing dependences.')
    os.system('pip install -r dependences.txt')
    print('[+] Dependences installed.')


if __name__ == '__main__':
    configuration_env()
    # get data bases
    dbs = []
    dbs.append(requests.get(
        'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museo.csv',
        auth=('user', 'pass')))
    dbs.append(requests.get(
        'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv',
        auth=('user', 'pass')))
    dbs.append(requests.get(
        'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv',
        auth=('user', 'pass')))

    get_status_request(dbs)
    conection = conection_to_postgres()
    cursor = conection.cursor()
    cursor.execute(f'''
        SELECT datname
        FROM pg_database
        WHERE datistemplate = false;
        ''')
    databases = cursor.fetchall()


    if ('datos_argentina',) in databases:
        print(f'[+] Data base "datos_argentina" exists on postgresql.')
    else:
        print(f'[+] Data base "datos_argentina" not exists on postgresql.')
        cursor.execute('CREATE DATABASE "datos_argentina";')
        print(f'[+] Data base "datos_argentina" no was found, then was created.')
    print()

    conection.close()

    try:
        conection = conection_to_postgres()
        print(f'[+] Connection to Postgresql was completed.')
    except:
        print(f'[+] We can not connect with Postgresql.')
        exit()
    print()

    # we connect with data base datos argentina
    conection = conection_to_db()
    cursor = conection.cursor()
    cursor.execute(f'''
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public';
                    ''')
    tables = cursor.fetchall()
    print(f'[+] Connection with data base was completed correctly.')
    print()

    control_table('normal_biblioteca_cine_museo', tables)

    for db in dbs:
        # creation of paths
        if not os.path.isdir(f'{get_name_url(db)}'):
            os.mkdir(f'{get_name_url(db)}')
            if not os.path.isdir(f'{get_name_url(db)}/{now.strftime("%Y-%B")}/'):
                os.mkdir(f'{get_name_url(db)}/{now.strftime("%Y-%B")}/')

        with open(f'{get_name_url(db)}/{now.strftime("%Y-%B")}/{get_name_url(db)}-{now.strftime("%d-%m-%Y")}.csv', 'w',
                  encoding=f'{db.encoding}') as file:
            file.write(db.text)

        # load data to be processing
        df = pd.read_csv(
            filepath_or_buffer=
            f'{get_name_url(db)}/{now.strftime("%Y-%B")}/{get_name_url(db)}-{now.strftime("%d-%m-%Y")}.csv',
            sep=',')

        # save values relevant from data frame.
        cod_localidad, id_provincia, id_departamento, categoria, provincia, localidad, nombre, domicilio, \
        codigo_postal, numero_de_telefono, mail, web = assign_normal_table(get_name_url(db), df)

        # for each row of csv
        insert_normal_table(
            cod_localidad, id_provincia, id_departamento, categoria, provincia, localidad, nombre, domicilio,
            codigo_postal, numero_de_telefono, mail, web, get_name_url(db))

    # Statitics category
    control_table('estadistica_categoria', tables)

    cursor.execute(f'''
    SELECT categoria, COUNT(*)
    FROM normal_biblioteca_cine_museo
    GROUP BY categoria;''')
    cant_registros_categoria = cursor.fetchall()
    exception_exists, exception_undifined, new_rows = 0, 0, 0
    for rw in cant_registros_categoria:
        try:
            cursor.execute(f'''
            INSERT INTO estadistica_categoria(fecha_carga,categoria,cant_registros)
            VALUES('{now.strftime("%Y-%m-%d")}', '{rw[0]}', {rw[1]});''')
            new_rows += 1
        except psycopg2.errors.UniqueViolation:
            exception_exists += 1
        except:
            exception_undifined += 1
    print(f'[+] Was inserted on "estadistica_categoria" {new_rows} new rows as result from analysis table '
          f'normal_biblioteca_cine_museo.')
    print(f'[+] Was not inserted on "estadistica_categoria" {exception_exists} rows because now exists.')
    print(f'[-] Was not inserted on "estadistica_categoria" {exception_undifined} rows because apper an error.')

    # statitics cine
    control_table('estadistica_cine', tables)
    # # query to import data from given csv
    df = pd.read_csv(filepath_or_buffer=f'cine/{now.strftime("%Y-%B")}/cine-{now.strftime("%d-%m-%Y")}.csv', sep=',')

    # save values relevant from data frame.
    provincia = tuple(df["Provincia"].values)
    butacas = tuple(df["Butacas"].values)
    pantallas = tuple(df["Pantallas"].values)
    espacio_incaa = tuple(df["espacio_INCAA"].values)

    # create a temporary table
    try:
        cursor.execute(f'''
        CREATE TABLE estadistica_cine_temp(
        id SERIAL,
        provincia VARCHAR(40) NOT NULL,
        butacas INT NOT NULL,
        pantallas INT NOT NULL,
        espacios_incaa INT NOT NULL,
        PRIMARY KEY(id));''')
    except:
        cursor.execute(f'''DELETE FROM estadistica_cine_temp;''')

    for rw in range(len(provincia)):
        cursor.execute(f'''
        INSERT INTO estadistica_cine_temp(provincia,butacas,pantallas,espacios_incaa)
        VALUES('{provincia[rw]}',{control_null(str(butacas[rw]))},{control_null(str(pantallas[rw]))},
        {control_null_incaa(str(espacio_incaa[rw]))});''')

    cursor.execute(f'''
    SELECT provincia, SUM(butacas), SUM(pantallas), SUM(espacios_incaa)
    FROM estadistica_cine_temp GROUP BY provincia;''')

    estadistico_cine = cursor.fetchall()
    exception_exists, exception_undifined, new_rows = 0, 0, 0
    for rw in estadistico_cine:
        try:
            cursor.execute(f'''
            INSERT INTO estadistica_cine(fecha_carga,provincia,cant_butacas,cant_pantallas,cant_espacios_inca)
            VALUES('{now.strftime("%Y-%m-%d")}','{rw[0]}',{rw[1]},{rw[2]},{rw[3]});''')
            new_rows += 1
        except psycopg2.errors.UniqueViolation:
            exception_exists += 1
        except:
            exception_undifined += 1
    print(f'[+] Was inserted on "estadistica_cine" {new_rows} new rows as result from analysis csv cine.')
    print(f'[+] Was not inserted on "estadistica_cine" {exception_exists} rows because now exists.')
    print(f'[-] Was not inserted on "estadistica_cine" {exception_undifined} rows because apper an error.')

    cursor.execute(f'''DROP TABLE estadistica_cine_temp;''')

    # close connection
    conection.commit()
    conection.close()

