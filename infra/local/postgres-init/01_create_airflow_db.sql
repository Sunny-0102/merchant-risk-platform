SELECT 'CREATE DATABASE airflow OWNER app'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'airflow') \gexec
