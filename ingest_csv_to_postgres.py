def gcs_to_postgres(request):
    import os
    import sqlalchemy
    import pandas as pd
    import pg8000
    import io
    
    from google.cloud import storage
    from google.cloud.sql.connector import Connector, IPTypes
    from datetime import datetime, timedelta 

    def connect_with_connector() -> sqlalchemy.engine.base.Engine:

        """
        Initializes a connection pool for a Cloud SQL instance of Postgres.
        Uses the Cloud SQL Python Connector package. (Provided by Google)
        """
        instance_connection_name = os.environ["INSTANCE_CONNECTION_NAME"]
        db_user = os.environ["DB_USER"]
        db_pass = os.environ["DB_PASS"]
        db_name = os.environ["DB_NAME"]
        ip_type = IPTypes.PRIVATE if os.environ.get("PRIVATE_IP") else IPTypes.PUBLIC
        query_string = dict({"unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(instance_connection_name)})

        connector = Connector()

        def getconn() -> pg8000.dbapi.Connection:
            conn: pg8000.dbapi.Connection = connector.connect(
                instance_connection_name,
                "pg8000",
                user=db_user,
                password=db_pass,
                db=db_name,
                ip_type=ip_type,
            )
            return conn

        pool = sqlalchemy.create_engine(
            "postgresql+pg8000://",
            creator=getconn,
            connect_args=query_string
        )
        return pool

    def ingest_csv_to_postgres(request):        
        pool = connect_with_connector()
        conn = pool.connect()

        try:
            #stored file variables
            storage_client = storage.Client()
            bucket_name = "weather_payload"  
            bucket = storage_client.bucket(bucket_name)
            suffix = "_forecast_current.csv"  

            #define rolling 7 day lookback for batch processing
            rolling_seven = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')
            int_rolling_seven = int(rolling_seven)

            for blob in bucket.list_blobs():
                file_date_part = blob.name.split('_')[-3] 
                int_file_date = int(file_date_part)
                if suffix in blob.name and int_file_date >= int_rolling_seven: #compare file date conditional to 7 day lookback
                    blob_content = blob.download_as_string()
                    csv_data = io.StringIO(blob_content.decode('utf-8'))
                    data = pd.read_csv(csv_data)

                    #manually coerce datatypes to disallow inference
                    column_data_types = {
                        'id': 'int64',
                        'lat': 'float64',
                        'lon': 'float64',
                        'tz': 'string',
                        'date': 'object',
                        'units': 'string',
                        'current_temp': 'float64',
                        'feels_like': 'float64',
                        'pressure': 'float64',
                        'humidity': 'float64',
                        'dew_point': 'float64',
                        'uvi': 'float64',
                        'clouds': 'float64',
                        'visibility': 'float64',
                        'wind_speed': 'float64',
                        'wind_deg': 'float64',
                        'wind_gust': 'float64'
                    }
                    
                    for column, data_type in column_data_types.items():
                        data[column] = data[column].astype(data_type)

                    data['date'] = pd.to_datetime(data['date'])
                    #data['sunset'] = pd.to_datetime(data['sunset'])

                    #id duplication (uniquness constraint) checking
                    duplicates = data.duplicated(subset=['id'])
                    num_duplicates = duplicates.sum()
                    if num_duplicates > 0:
                        raise ValueError("Duplicate values found in the 'id' column.")

                    #id null value checking
                    null_values = data['id'].isnull()
                    num_null_values = null_values.sum()
                    if num_null_values > 0:
                        raise ValueError("Null values found in the 'id' column.")

                    #check if payload is appropriate size
                    num_rows = len(data)
                    if not (1 <= num_rows <= 10000):
                        raise ValueError("The number of rows in the DataFrame is not between 1 and 10000.")

                    #define table source and ingest function conditions
                    table_name = "weather_data_current"
                    data.to_sql(name=table_name, con=conn, if_exists='append', index=False)
                  
            #below code is for diagnosing unexpected behavior
            #sql_check = '''SELECT * FROM weather_data_current;'''
            #conn.execute(sql_check)
            #for i in conn.fetchall():
            #    print(i)

            #commit transaction with response message
            conn.commit()
            return "CSV data ingested into PostgreSQL successfully."

        except Exception as e:
            #rollback commit in the event of an error
            conn.rollback()
            return "An error occurred: {}".format(str(e))

        finally:
            conn.close()
            
    return ingest_csv_to_postgres(request)