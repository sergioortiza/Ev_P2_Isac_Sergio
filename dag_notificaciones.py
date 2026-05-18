from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

# Definición de funciones para las 4 etapas 
def etapa_ingesta():
    print(">>> INICIO ETAPA: INGESTA")
    print("Capturando interacciones desde la Red Social...")
    time.sleep(1)

def etapa_limpieza():
    print(">>> INICIO ETAPA: LIMPIEZA Y TRANSFORMACIÓN")
    print("Normalizando formatos de datos...")
    time.sleep(1)

def etapa_validacion():
    print(">>> INICIO ETAPA: VALIDACIÓN")
    print("Verificando integridad estructural y semántica...")
    time.sleep(1)

def etapa_carga():
    print(">>> INICIO ETAPA: CARGA")
    print("Enviando datos al motor de notificaciones...")
    time.sleep(1)

# Configuración del DAG
default_args = {
    'owner': 'Sergio_DataOps',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'pipeline_notificaciones_v2',
    default_args=default_args,
    description='Pipeline de notificaciones en tiempo real',
    schedule=None,  
    start_date=datetime(2026, 5, 17),
    catchup=False,
    tags=['evaluacion_ity1101'],
) as dag:

    # Definición de tareas del ciclo de vida de proyectos de datos 
    t1 = PythonOperator(task_id='ingesta', python_callable=etapa_ingesta)
    t2 = PythonOperator(task_id='limpieza', python_callable=etapa_limpieza)
    t3 = PythonOperator(task_id='validacion', python_callable=etapa_validacion)
    t4 = PythonOperator(task_id='carga', python_callable=etapa_carga)

    # Flujo del Pipeline DataOps 
    t1 >> t2 >> t3 >> t4