from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

algo = 'XC8fPf5qSodFuWgqKk+rwPyy/xjKu3g0CtNB0cd7kOM='

def tarea_inicio():
    tiempo_inicio = datetime.now()
    mensaje = f"Tarea 1. Hora de inicio del dag: {tiempo_inicio.strftime('%Y-%m-%d %H:%M:%S')}"
    print(mensaje) # Sigue saliendo en el log por si acaso

    time.sleep(1)
    
    # Lo que retornas aquí se guarda automáticamente en XCom
    return {
        "mensaje": mensaje,
        "timestamp": tiempo_inicio.timestamp()
    }

def tarea_final(ti):
    # 'ti' es el Task Instance que se inyecta automáticamente para consultar XComs
    datos_inicio = ti.xcom_pull(task_ids='tarea_inicio')
    
    tiempo_fin = datetime.now()
    inicio_ts = datos_inicio["timestamp"]
    fin_ts = tiempo_fin.timestamp()
    
    tiempo_transcurrido = fin_ts - inicio_ts
    
    mensaje_final = f"Tarea 2. Hora de finalización: {tiempo_fin.strftime('%Y-%m-%d %H:%M:%S')} | Tiempo transcurrido: {tiempo_transcurrido:.2f} segundos"
    print(mensaje_final)
    
    # También retornamos esto para guardarlo en los XComs de la tarea 2
    return {
        "mensaje_fin": mensaje_final,
        "tiempo_transcurrido_segundos": tiempo_transcurrido
    }

with DAG(
    dag_id='my_first_dag',
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=5),
    catchup=False,
    tags=['basico', 'ejemplo'],
) as dag:

    inicio = PythonOperator(
        task_id='tarea_inicio',
        python_callable=tarea_inicio,
    )

    fin = PythonOperator(
        task_id='tarea_final',
        python_callable=tarea_final,
    )

    inicio >> fin