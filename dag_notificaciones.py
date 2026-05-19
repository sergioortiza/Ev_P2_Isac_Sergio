from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

# 1. ETAPA DE INGESTA: Simulamos la llegada de interacciones "sucias"
def etapa_ingesta(**context):
    print(">>> INICIO ETAPA: INGESTA")
    # Datos de ejemplo: RUTs sin guion, IDs negativos y valores nulos
    datos_crudos = [
        {"usuario_rut": "12345678k", "tipo": "like", "interaccion_id": 101},
        {"usuario_rut": "198765432", "tipo": "follower", "interaccion_id": -5}, # ERROR: Negativo
        {"usuario_rut": "201234567", "tipo": "comment", "interaccion_id": 102},
        {"usuario_rut": None, "tipo": "like", "interaccion_id": 103}            # ERROR: Sin RUT
    ]
    print(f"Datos capturados de la Red Social: {len(datos_crudos)} registros.")
    return datos_crudos

# 2. ETAPA DE LIMPIEZA: Agregamos guiones a los RUTs
def etapa_limpieza(**context):
    print(">>> INICIO ETAPA: LIMPIEZA Y TRANSFORMACIÓN")
    datos = context['ti'].xcom_pull(task_ids='ingesta')
    datos_limpios = []
    
    for registro in datos:
        rut = registro.get("usuario_rut")
        if rut:
            # Lógica: Agregamos el guion antes del último dígito
            rut_formateado = f"{rut[:-1]}-{rut[-1]}"
            registro["usuario_rut"] = rut_formateado
            print(f"RUT transformado: {rut} -> {rut_formateado}")
        datos_limpios.append(registro)
    
    return datos_limpios

# 3. ETAPA DE VALIDACIÓN: Filtramos negativos y datos incompletos
def etapa_validacion(**context):
    print(">>> INICIO ETAPA: VALIDACIÓN SEMÁNTICA")
    datos = context['ti'].xcom_pull(task_ids='limpieza')
    datos_validados = []
    
    for registro in datos:
        es_valido = True
        # Validación 1: El ID no puede ser negativo
        if registro["interaccion_id"] < 0:
            print(f"ANOMALÍA DETECTADA: ID negativo en {registro}")
            es_valido = False
        
        # Validación 2: El RUT no puede ser nulo
        if registro["usuario_rut"] is None:
            print(f"ANOMALÍA DETECTADA: RUT faltante en {registro}")
            es_valido = False
            
        if es_valido:
            datos_validados.append(registro)
            
    print(f"Validación completada. Registros aptos: {len(datos_validados)} de {len(datos)}.")
    return datos_validados

# 4. ETAPA DE CARGA: Mostramos el resultado final listo para notificar
def etapa_carga(**context):
    print(">>> INICIO ETAPA: CARGA")
    datos_finales = context['ti'].xcom_pull(task_ids='validacion')
    print("ENVIANDO NOTIFICACIONES A LOS SIGUIENTES USUARIOS:")
    for d in datos_finales:
        print(f"Notificar a {d['usuario_rut']} por interaccion {d['interaccion_id']}")

default_args = {
    'owner': 'Sergio_DataOps',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'pipeline_notificaciones_v3', # Versión 3 con lógica real
    default_args=default_args,
    description='Pipeline con validación de RUT y limpieza de negativos',
    schedule=None,
    start_date=datetime(2026, 5, 18),
    catchup=False,
    tags=['evaluacion_ity1101'],
) as dag:

    t1 = PythonOperator(task_id='ingesta', python_callable=etapa_ingesta)
    t2 = PythonOperator(task_id='limpieza', python_callable=etapa_limpieza)
    t3 = PythonOperator(task_id='validacion', python_callable=etapa_validacion)
    t4 = PythonOperator(task_id='carga', python_callable=etapa_carga)

    t1 >> t2 >> t3 >> t4