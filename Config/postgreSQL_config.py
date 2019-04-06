import psycopg2 as pc
import io
import pandas as pd

## Make column names database appropriate
def clean_columns(columns):
    cols = []
    for col in columns:
        col = str(col).lower().replace(' ', '_')
        cols.append(col)
    return cols

## Config postgreSQL connection

## sql_statements is a list of type string, replace password with 
def execute_in_postgresql(sql_statements \
                        ,dbname='dev' \
                        ,endpoint='kingcountryrugby.cn4rvlib7x10.ap-southeast-2.rds.amazonaws.com' \
                        ,port='5432' \
                        ,user='MrJones' \
                        ,password="""***Insert password here***"""):

    ## Init connection
    postgresql_connection = None

    try:
        ## Create connection to postgreSQL instance
        postgresql_connection = pc.connect(host=endpoint, port=port, database=dbname, user=user, password=password)
        print('Connection created')

        ## Create a cursor
        cur = postgresql_connection.cursor()

        ## Execute list of statements
        for statement in sql_statements:
            cur.execute(statement)

    except (Exception, pc.DatabaseError) as error:
        print(error)

    finally:
        if postgresql_connection is not None:
            ## Save changes made to db
            postgresql_connection.commit()
            ## End connection
            postgresql_connection.close()
            print('Database connection closed.')

def csv_to_postgresql(file_path \
                    ,table_name \
                    ,delimiter=',' \
                    ,has_headers=True
                    ,dbname='dev' \
                    ,endpoint='kingcountryrugby.cn4rvlib7x10.ap-southeast-2.rds.amazonaws.com' \
                    ,port='5432' \
                    ,user='MrJones' \
                    ,password="""***Insert password here***"""):

    try:
        ## Create connection to postgreSQL instance
        postgresql_connection = pc.connect(host=endpoint, port=port, database=dbname, user=user, password=password)
        print('Connection created')

        ## Create a cursor
        cur = postgresql_connection.cursor()

        ## Open text file
        with open(file_path, 'r') as file:
            # Skip first row if column names
            if has_headers:
                next(file)
            cur.copy_from(file, table_name, sep=delimiter)

    except (Exception, pc.DatabaseError) as error:
        print(error)

    finally:
        if postgresql_connection is not None:
            ## Save changes made to db
            postgresql_connection.commit()
            ## End connection
            postgresql_connection.close()
            print('Database connection closed.')

## For inserts only at the moment
def pandas_to_postgres(df \
                        ,table_name
                        ,dbname='dev' \
                        ,endpoint='kingcountryrugby.cn4rvlib7x10.ap-southeast-2.rds.amazonaws.com' \
                        ,port='5432' \
                        ,user='MrJones' \
                        ,password="""***Insert password here***"""):

    try:
        ## Create connection to postgreSQL instance
        postgresql_connection = pc.connect(host=endpoint, port=port, database=dbname, user=user, password=password)
        print('Connection created')

        ## Init empty string buffer opject
        data = io.StringIO()

        ## Scrub column names
        df.columns = clean_columns(df.columns)

        ## Write df to string buffer as flat file
        df.to_csv(data, header=False, index=False)
        data.seek(0)

        ## Create curser object
        curs = postgresql_connection.cursor()

        ## Plonk csv into postgresql
        curs.copy_from(data, table_name, sep=',')

    except (Exception, pc.DatabaseError) as error:
        print(error)

    finally:
        if postgresql_connection is not None:
            ## Save changes made to db
            postgresql_connection.commit()
            ## End connection
            postgresql_connection.close()
            print('Database connection closed.')
