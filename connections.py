class string(object):    
    folder_raw = '/home/jc/airflow/Economics_BR/Raw'
    folder_staging_Files = '/home/jc/airflow/Economics_BR/Staging_Files'
    folder_local_dw = '/home/jc/airflow/Economics_BR/local_dw'
    folder_curated = '/home/jc/airflow/Economics_BR/Curated'
    url_fundamentus = 'https://www.fundamentus.com.br/resultado.php'
    cnx_string = 'postgresql://postgres:postgres@127.0.0.1/teste_airflow'  
    cnx_stringRDS = 'postgresql://knppost_b3gres:?Wli17j674@sala443-projeto-b3.ckp5huelvu5f.us-east-1.rds.amazonaws.com/dw_b3'  
    s3_bucket = "datalake-josecarlos-977711486277"
    s3_folder = "raw-data/"
    s3_folder_parquet = "s3://datalake-josecarlos-977711486277/parquet_data/"
    
    


