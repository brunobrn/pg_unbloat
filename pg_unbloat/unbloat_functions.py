import psycopg

from config import host,dbname,user,password,port,app_name,table_min_size,table_max_size
from queries import *

# This function get the name of all tables using the select GET_TABLES in queries.py
def get_tables():
    try:
        conn = psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port, application_name=app_name)
        cur = conn.execute(GET_SCHEMAS)
        SCHEMAS = cur.fetchall()
        tables = []

        for x in SCHEMAS:
            cur.execute('''set search_path to %s''' % (x))
            cur.execute(GET_TABLES)
            table_schema = cur.fetchall()
            
            for i in range(len(table_schema)):
                schema = table_schema[i][0]
                table = table_schema[i][1]
                if table_schema[i][2] >= table_min_size and table_schema[i][2] <= table_max_size:
                    tables.append(schema+"."+table)

        # End transacations
        cur.close()
        return tables

    except (Exception, psycopg.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

# This function get the parcial stat tuple of tables in size range
def exec_stattuple_approx(table_name):
    try:
        conn = psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port, application_name=app_name)
        cur = conn.execute('''select * from pgstattuple_approx('%s')''' % (table_name))
        approx_result = cur.fetchall()
        cur.close()

        return approx_result

    except (Exception, psycopg.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

# This function get the full stat tuple of tables elected
def exec_stattuple(table_name):
    try:
        conn = psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port, application_name=app_name)
        cur = conn.execute('''select * from pgstattuple('%s')''' % (table_name))
        stattuple_result = cur.fetchall()
        cur.close()

        return stattuple_result

    except (Exception, psycopg.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

# Create the necessary functions
def init_migration():
    try:
        conn = psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port, application_name=app_name)
        cur = conn.cursor()
        cur.execute(open("init_migration.sql", "r").read())
        conn.commit()
        print("#################\nMigration executed")
        cur.close()

    except (Exception, psycopg.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

# Clean off the migration
def clean_migration():
    try:
        conn = psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port, application_name=app_name)
        cur = conn.cursor()
        cur.execute(open("clean_migration.sql", "r").read())
        conn.commit()
        print("#################\nObjects from migration sucessefully removed")
        cur.close()

    except (Exception, psycopg.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

# This function get the ddl of table to be analyzed
def ddl_creator(table_name):
    try:
        conn    = psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port, application_name=app_name)
        cur     = conn.cursor()

        cur     = conn.execute('''select create_table_1 from public.get_ddl('%s')''' % (table_name))
        ddl_1   = cur.fetchall()

        cur     = conn.execute('''select create_table_2 from public.get_ddl('%s')''' % (table_name))
        ddl_2   = cur.fetchall()

        cur     = conn.execute('''select create_table_3 from public.get_ddl('%s')''' % (table_name))
        ddl_3   = cur.fetchall()

        cur                 = conn.execute('''select created_table_name from public.get_ddl('%s')''' % (table_name))
        ddl_4               = cur.fetchall()
       

        full_ddl            = ddl_1[0][0]+ddl_2[0][0]+ddl_3[0][0]
        created_table_name  = ddl_4[0][0]

        cur.close()
        return full_ddl,created_table_name

    except (Exception, psycopg.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

# Create a copy table with the data of main table
def create_copy_table(ddl):
    try:
        conn = psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port, application_name=app_name)
        cur = conn.execute(ddl)
        conn.commit()
        cur = conn.cursor()
        
        cur.close()

    except (Exception, psycopg.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

# Drop the copied table to release storage
def drop_copy_table(table_copy_name):
    try:
        conn = psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port, application_name=app_name)
        cur = conn.execute("drop table if exists %s" % (table_copy_name))
        conn.commit()
        print("Temp table %s droped" % (table_copy_name))
        cur = conn.cursor()
        
        cur.close()

    except (Exception, psycopg.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

# Get the real size of table in bytes
def get_table_size(table_name):
    try:
        conn    = psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port, application_name=app_name)
        cur     = conn.cursor()

        strValue = table_name
        ch = '.'
        # Remove all characters before the character '.' from string
        listOfWords = strValue.split(ch, 1)
        if len(listOfWords) > 0: 
            strValue = listOfWords[1]


        cur     = conn.execute('''select SIZE_IN_BYTES from public.get_table_size('%s')''' % (strValue))
        size_in_bytes = cur.fetchall()[0][0]

        cur.close()
        return size_in_bytes
    except (Exception, psycopg.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
