# Guía completa: Pipeline de Airflow en AWS con ECS, RabbitMQ, RDS y Secrets Manager

**Región:** Ohio (`us-east-2`)\
**Nivel:** principiante, paso a paso\
**Objetivo:** aprender a desplegar un pipeline pequeño y comprender cómo
se comunican sus componentes.\
**Prioridad:** minimizar costos y evitar recursos olvidados.

> **Importante sobre costos:** esta guía no promete que el proyecto sea
> gratuito. EC2, RDS, Amazon MQ, Secrets Manager, almacenamiento,
> transferencia de datos y registros pueden generar cargos. La
> disponibilidad de créditos y la capa gratuita depende del tipo,
> antigüedad y condiciones de la cuenta. Revisa el precio vigente en la
> región `us-east-2` antes de crear cada recurso. Un presupuesto de AWS
> genera alertas, pero no detiene automáticamente el consumo.

------------------------------------------------------------------------

## 1. Qué vamos a construir

El pipeline tendrá tres tareas ejecutadas en orden:

1.  `Hola, iniciando pipeline a las HH:MM:SS DD:MM:AAAA`
2.  `Estamos en la tarea 2 y vamos a aplicar un delay de 2seg`
3.  `Pipeline finalizado a las HH:MM:SS DD:MM:AAAA`

Usaremos la zona horaria `America/Bogota`. La tarea 2 esperará dos
segundos con `time.sleep(2)`. Los mensajes se escribirán en los logs de
Airflow.

### Arquitectura elegida

-   **EC2 (maestro):** ejecuta Docker Compose con Airflow Scheduler y
    Webserver; también puede ejecutar componentes auxiliares como
    Triggerer si la versión/configuración lo requiere.
-   **ECS (dos workers):** dos tareas de ECS ejecutan el proceso de
    worker de Airflow. No son dos instancias EC2 independientes.
-   **ECR:** almacena la imagen Docker de Airflow que se usará tanto en
    el maestro como en los workers.
-   **Amazon MQ for RabbitMQ:** broker que recibe y distribuye los
    mensajes de trabajo de Celery.
-   **RDS PostgreSQL:** base de datos de metadatos de Airflow; guarda
    estados de DAGs, tareas, ejecuciones y configuración interna.
-   **AWS Secrets Manager:** guarda contraseñas y claves sensibles.
-   **IAM:** controla qué componente puede leer cada secreto y acceder a
    otros servicios.
-   **CloudWatch Logs:** recibe logs de contenedores si se configura la
    integración.

**Flujo de ejecución:** el Scheduler decide qué tarea está lista y la
envía mediante Celery a RabbitMQ. Un worker de ECS consume el trabajo,
ejecuta el código y registra su estado en PostgreSQL a través de la
configuración de Airflow. El Webserver muestra el estado y los logs
disponibles.

**Importante:** ECR no ejecuta contenedores; almacena imágenes. ECS
ejecuta las tareas. RabbitMQ no reemplaza RDS: RabbitMQ es la cola de
mensajes, mientras que RDS es la base de datos de metadatos.

### Por qué `CeleryExecutor`

Airflow con `CeleryExecutor` permite distribuir tareas a workers que
consumen una cola mediante un broker compatible, como RabbitMQ. Todos
los componentes deben usar versiones compatibles de Airflow, Python y
dependencias, así como la misma configuración esencial. No mezcles
versiones de la imagen entre maestro y workers.

------------------------------------------------------------------------

## 2. Antes de empezar: seguridad, región y presupuesto

### 2.1 Elegir región

En la consola de AWS, arriba a la derecha, selecciona **US East (Ohio)
--- `us-east-2`**. Crea los recursos del laboratorio en esa región para
evitar complejidad de red y transferencia entre regiones.

### 2.2 Proteger la cuenta

1.  Inicia sesión en la [consola de
    AWS](https://console.aws.amazon.com/).
2.  Activa MFA para el usuario raíz.
3.  No crees claves de acceso para el usuario raíz.
4.  Para el trabajo diario, utiliza un usuario/rol administrativo con
    permisos adecuados; para los recursos en ejecución, utiliza roles
    IAM específicos.
5.  No compartas claves, contraseñas, tokens, ARN sensibles ni capturas
    que los expongan.

### 2.3 Revisar plan y créditos

Abre [Billing and Cost
Management](https://console.aws.amazon.com/costmanagement/). Revisa el
tipo de plan, los créditos, las fechas de vencimiento y cualquier aviso
de facturación. Las condiciones de gratuidad dependen de la cuenta y del
servicio; no asumas que RDS o Amazon MQ están incluidos sin costo.

### 2.4 Crear un presupuesto de alerta

En **Billing → Budgets → Create budget**, crea un presupuesto mensual de
referencia llamado `airflow-rabbitmq-lab-budget`, por ejemplo de **USD
5**, con avisos de gasto real al 50 %, 80 % y 100 %. Configura el correo
y confirma las alertas.

Este importe es solo un umbral de aviso, no una estimación del costo
total ni un límite de gasto. Las alertas pueden llegar con retraso y no
apagan recursos.

### 2.5 Revisar recursos existentes

Aunque la cuenta parezca nueva, revisa las consolas de EC2, RDS, ECS,
Amazon MQ, NAT Gateways, Elastic IPs, volúmenes EBS y CloudWatch. No
borres recursos que no reconozcas; primero identifica su propietario y
función.

------------------------------------------------------------------------

## 3. Diseñar la red antes de crear servicios

Para un laboratorio seguro, los componentes que se comunican entre sí
deberían estar en una VPC y, preferiblemente, en subredes privadas
cuando no necesiten acceso directo desde Internet.

Una distribución típica:

-   **EC2 maestro:** puede estar en una subred pública con acceso
    administrativo restringido, o privada si se configura un método de
    administración seguro.
-   **RDS PostgreSQL:** subredes privadas; no debe ser públicamente
    accesible.
-   **Amazon MQ RabbitMQ:** acceso privado desde los componentes de
    Airflow.
-   **ECS workers:** subredes con conectividad hacia RDS, RabbitMQ, ECR
    y CloudWatch.
-   **Security Groups:** permiten solo el tráfico necesario entre
    grupos, no desde cualquier dirección.

### Puertos habituales

-   SSH a EC2: TCP `22`, solo desde tu IP pública cuando sea necesario;
    no abrir a `0.0.0.0/0`.
-   Airflow Webserver: normalmente TCP `8080`, restringido a tu IP o
    accesible mediante un método privado.
-   PostgreSQL: TCP `5432`, permitido desde el Security Group del
    maestro y de los workers.
-   RabbitMQ AMQPS: normalmente TCP `5671` para conexiones TLS; usa el
    endpoint y puerto exactos que muestre Amazon MQ.
-   Interfaz de administración de RabbitMQ: no la expongas públicamente.
    Si hace falta, accede por una vía privada y temporalmente
    controlada.

**No configures RDS ni RabbitMQ como públicos para "hacer que
funcione".** Si un componente no conecta, comprueba rutas, subredes,
DNS, Security Groups, credenciales y TLS.

### Nota de costos de red

Un NAT Gateway puede tener un costo fijo por hora y cargos por datos.
Para un laboratorio económico, no lo crees automáticamente. Primero
decide cómo obtendrán los workers las imágenes de ECR, cómo descargarán
dependencias y cómo enviarán logs. Los endpoints VPC también pueden
tener costos. Compara el costo de red con la necesidad de seguridad y
conectividad; no sacrifiques la seguridad haciendo públicos RDS o
RabbitMQ.

------------------------------------------------------------------------

## 4. Plan de fases

Avanza en este orden y verifica cada fase antes de seguir:

1.  Preparación de cuenta, región, MFA y presupuesto.
2.  Secrets Manager e IAM.
3.  VPC, subredes, rutas y Security Groups.
4.  RDS PostgreSQL para metadatos de Airflow.
5.  Amazon MQ con RabbitMQ.
6.  Proyecto local: Dockerfile, dependencias, configuración y DAG.
7.  Construir y probar localmente la imagen.
8.  Crear ECR y publicar la imagen.
9.  Crear EC2 y desplegar el maestro de Airflow con Docker Compose.
10. Crear ECS Cluster, Task Definition y dos tareas worker.
11. Probar conectividad, ejecutar el DAG y validar logs.
12. Revisión de seguridad, costos y eliminación de recursos al terminar.

No es necesario crear todos los servicios en una sola sesión.

------------------------------------------------------------------------

## 5. Secrets Manager: qué guardar

Guarda contraseñas y claves en Secrets Manager; no las escribas en el
código, en el Dockerfile, en `docker-compose.yml`, en Git ni en logs.

Secretos que necesitaremos:

-   `airflow/rds`: nombre de usuario, contraseña, host, puerto y nombre
    de la base de datos, si elegimos guardarlos juntos.
-   `airflow/rabbitmq`: usuario, contraseña y endpoint/puerto del broker
    si la configuración de despliegue lo requiere.
-   `airflow/fernet`: clave Fernet de Airflow, compartida por todos los
    componentes que la necesiten.
-   `airflow/webserver`: clave secreta de la aplicación web, compartida
    por los componentes pertinentes.

Una organización alternativa consiste en separar credenciales de
parámetros de conexión. Evita duplicar secretos innecesariamente. No
todos los campos de conexión son secretos, pero puedes guardar un objeto
JSON para facilitar el despliegue.

### IAM para secretos

Crea roles distintos para EC2 y ECS. Concede únicamente
`secretsmanager:GetSecretValue` para los ARN específicos que cada rol
necesita. Si el secreto usa una clave KMS administrada por el cliente,
puede hacer falta permiso `kms:Decrypt`. No otorgues `secretsmanager:*`
sobre todos los secretos.

-   **Rol de instancia EC2:** permisos para leer los secretos que
    necesita el maestro y permisos de logs/administración estrictamente
    necesarios.
-   **Execution role de ECS:** permisos usados por ECS para obtener
    imágenes y, si se configuran secretos en la definición de tarea,
    recuperarlos e inyectarlos al iniciar.
-   **Task role de ECS:** permisos disponibles para el proceso dentro
    del contenedor; úsalo si el propio worker consulta Secrets Manager
    durante la ejecución.

El execution role y el task role tienen funciones diferentes. No
incrustes credenciales AWS estáticas en variables de entorno.

### Variables de entorno

Separa: - **No sensibles:** versión de Airflow, zona horaria, nombre del
servicio, nivel de log. - **Sensibles:** contraseñas de RDS/RabbitMQ,
Fernet key y webserver secret.

Si ECS inyecta secretos como variables de entorno al iniciar un
contenedor, normalmente los cambios en el secreto no se reflejan en un
contenedor ya ejecutándose: tendrás que desplegar/reiniciar las tareas
para recoger el valor nuevo. Protege los logs para que nunca impriman
variables sensibles.

Secrets Manager tiene costo por secreto y por ciertas llamadas API.
Verifica el precio vigente y evita crear muchos secretos separados sin
necesidad.

------------------------------------------------------------------------

## 6. RDS PostgreSQL para Airflow

Crea una instancia PostgreSQL compatible con la versión que admita la
versión de Airflow seleccionada. Antes de elegir la versión, consulta la
matriz de compatibilidad de Airflow.

Configuración orientativa para un laboratorio:

-   Región: `us-east-2`.
-   Motor: PostgreSQL.
-   Base de datos inicial: por ejemplo, `airflow`.
-   Acceso público: **No**.
-   VPC: la misma del maestro y los workers.
-   Security Group: permite TCP `5432` desde el Security Group del
    maestro y desde el de los workers.
-   Credenciales: almacenadas en Secrets Manager.
-   Almacenamiento, alta disponibilidad y tamaño: elige la opción
    compatible de menor costo que esté disponible para tu cuenta, sin
    asumir que será gratuita.

Airflow debe tener una base de metadatos dedicada. No uses una base de
datos SQLite para este despliegue distribuido.

### Prueba de conexión

Antes de desplegar todo Airflow, comprueba que el maestro y los workers
pueden resolver el hostname privado y alcanzar el puerto `5432`. Después
prueba autenticación con el usuario de Airflow, sin imprimir la
contraseña en la terminal ni en logs.

Si falla, revisa: 1. Host y puerto. 2. Usuario, contraseña y nombre de
base de datos. 3. Reglas de entrada del Security Group de RDS. 4. Rutas,
subredes, DNS y conectividad. 5. TLS y configuración del cliente.

No abras PostgreSQL a todo Internet como solución temporal.

------------------------------------------------------------------------

## 7. Amazon MQ con RabbitMQ

Crea un broker de Amazon MQ compatible con RabbitMQ. Selecciona la
opción de menor costo que esté disponible y que sirva para el
laboratorio; la disponibilidad de tamaños, versiones y opciones depende
de la región y del servicio vigente.

-   Mantén el broker en la misma VPC que Airflow.
-   Limita las conexiones entrantes al Security Group de EC2 y al de los
    workers.
-   Usa el endpoint AMQPS/TLS y el puerto que indique la consola.
-   Guarda el usuario y la contraseña en Secrets Manager.
-   No publiques la consola de administración de RabbitMQ en Internet.

Airflow/Celery debe configurarse con un `broker_url` compatible con el
endpoint TLS del broker. La sintaxis exacta depende del modo de
autenticación y del endpoint entregado por Amazon MQ. No inventes el
hostname: cópialo desde la consola y configura correctamente la
validación del certificado TLS.

### Prueba antes de continuar

Desde EC2, verifica resolución DNS y conectividad TCP al endpoint y
puerto del broker. Después prueba una conexión AMQPS usando un cliente
compatible. Que el puerto responda no garantiza que las credenciales,
TLS o permisos de RabbitMQ sean correctos.

------------------------------------------------------------------------

## 8. Proyecto de Airflow y Docker

Mantén una única versión de Airflow para maestro y workers. Fija las
versiones en el archivo de dependencias; evita usar `latest` para un
proyecto reproducible.

Estructura sugerida:

``` text
airflow-aws-lab/
├── dags/
│   └── simple_pipeline.py
├── config/
│   └── airflow.cfg        # solo si hace falta; sin secretos
├── requirements.txt
├── Dockerfile
├── docker-compose.yml     # para pruebas locales o maestro, sin secretos incrustados
├── .env.example           # nombres de variables y valores ficticios
├── .gitignore
└── README.md
```

El archivo `.env` real debe estar excluido de Git. No incluyas
contraseñas reales en `.env.example`.

### Ejemplo de DAG

El siguiente ejemplo usa la API clásica de Airflow 2.x. Si eliges
Airflow 3.x, revisa la documentación y ajusta imports y dependencias a
esa versión antes de construir la imagen. No mezcles código de Airflow 2
y 3.

``` python
from datetime import datetime
import time
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

TZ = pendulum.timezone("America/Bogota")


def tarea_1():
    ahora = pendulum.now(TZ).format("HH:mm:ss DD:MM:YYYY")
    print(f"Hola, iniciando pipeline a las {ahora}")


def tarea_2():
    print("Estamos en la tarea 2 y vamos a aplicar un delay de 2seg")
    time.sleep(2)


def tarea_3():
    ahora = pendulum.now(TZ).format("HH:mm:ss DD:MM:YYYY")
    print(f"Pipeline finalizado a las {ahora}")


with DAG(
    dag_id="pipeline_sencillo_aws",
    description="Pipeline de prueba con tres tareas secuenciales",
    start_date=pendulum.datetime(2026, 1, 1, tz=TZ),
    schedule=None,
    catchup=False,
    tags=["aprendizaje", "aws"],
) as dag:
    inicio = PythonOperator(
        task_id="tarea_1_inicio",
        python_callable=tarea_1,
    )

    espera = PythonOperator(
        task_id="tarea_2_delay",
        python_callable=tarea_2,
    )

    fin = PythonOperator(
        task_id="tarea_3_final",
        python_callable=tarea_3,
    )

    inicio >> espera >> fin
```

La expresión `inicio >> espera >> fin` establece el orden.
`schedule=None` significa que el DAG no se ejecuta automáticamente según
un calendario; puedes lanzarlo manualmente desde la interfaz. Las fechas
de inicio y el calendario controlan el scheduling, no la fecha impresa
por `pendulum.now()`.

### Dockerfile conceptual

La imagen debe contener la versión fijada de Airflow, Python y las
dependencias requeridas. El ejemplo siguiente es un punto de partida, no
una imagen final lista para producción:

``` dockerfile
FROM apache/airflow:2.10.5-python3.11

COPY --chown=airflow:root requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
    "apache-airflow==2.10.5" \
    -r /requirements.txt

COPY --chown=airflow:root dags/ /opt/airflow/dags/
```

Ajusta la versión según la compatibilidad real que hayas decidido y fija
las dependencias. En una imagen basada en la oficial de Airflow, sigue
sus recomendaciones para instalar paquetes y mantener la versión de
Airflow consistente. La etiqueta del ejemplo es ilustrativa y no
constituye una recomendación de usar una versión antigua sin revisar
soporte y compatibilidad.

Los DAGs deben estar disponibles en los componentes que los necesitan.
Para el primer laboratorio puedes incluirlos en la imagen y
reconstruirla al cambiar el DAG. Más adelante puedes estudiar un
mecanismo de sincronización como EFS si realmente lo necesitas.

------------------------------------------------------------------------

## 9. Configurar Airflow con CeleryExecutor

Los componentes deben compartir, de forma consistente:

-   `executor = CeleryExecutor`.
-   La misma base de metadatos de PostgreSQL.
-   La misma clave Fernet.
-   La misma clave secreta del webserver donde corresponda.
-   La misma configuración de broker y las dependencias necesarias.
-   Una versión compatible de Airflow y de los providers.
-   El mismo DAG y el código de las tareas.

El maestro ejecuta al menos Scheduler y Webserver. Los workers ejecutan
el proceso de worker de Celery y deben permanecer activos para consumir
tareas. El Triggerer solo es necesario para ciertos tipos de
operadores/deferrable tasks; para este pipeline sencillo no hace falta
añadirlo salvo que la configuración elegida lo requiera.

No ejecutes un worker de Airflow en cada contenedor del maestro si el
objetivo es demostrar que los workers reales están en ECS. Mantén clara
la separación entre maestro y workers.

### Configuración de conexiones

La configuración de Airflow incluye variables como la URL SQLAlchemy
para PostgreSQL y la URL del broker Celery. Los valores deben
construirse con cuidado para codificar caracteres especiales de
contraseñas, respetar TLS y evitar imprimir secretos.

No pongas las URLs con contraseña directamente en el repositorio. Obtén
los valores desde Secrets Manager durante el despliegue, mediante IAM y
un mecanismo de inyección seguro.

------------------------------------------------------------------------

## 10. Crear ECR y publicar la imagen

1.  Abre la consola de ECR en `us-east-2`.
2.  Crea un repositorio privado, por ejemplo `airflow-lab`.
3.  Construye la imagen localmente con el Dockerfile.
4.  Prueba localmente que el DAG se carga y que la imagen ejecuta los
    comandos de Airflow.
5.  Autentica Docker con ECR usando AWS CLI y las credenciales/rol
    apropiados.
6.  Etiqueta la imagen con el URI de ECR y una etiqueta versionada, por
    ejemplo `v1`.
7.  Sube la imagen.
8.  Comprueba en la consola que la imagen aparece en el repositorio.

No compartas el token de autenticación de ECR ni credenciales AWS. Evita
usar etiquetas mutables como `latest` para el despliegue final: usa una
etiqueta o digest identificable.

ECR puede cobrar por almacenamiento de imágenes y transferencia. Elimina
versiones que no necesites respetando la política de retención que
elijas.

------------------------------------------------------------------------

## 11. Desplegar el maestro en EC2

1.  Crea una instancia EC2 en `us-east-2` con un sistema operativo
    compatible con Docker.
2.  Selecciona un tipo pequeño que se ajuste a la memoria requerida por
    Airflow; no supongas que el tipo más pequeño podrá ejecutar todos
    los componentes cómodamente.
3.  Configura un Security Group con el acceso administrativo mínimo.
4.  Asocia un IAM Instance Profile al rol de EC2; no guardes access keys
    en el disco.
5.  Instala Docker y el plugin de Docker Compose según la documentación
    oficial de la distribución.
6.  Descarga la imagen desde ECR.
7.  Prepara la configuración de Compose para Scheduler y Webserver;
    configura el acceso a RDS y RabbitMQ.
8.  Recupera los secretos usando el rol de la instancia y el método
    seguro elegido.
9.  Ejecuta la migración de la base de metadatos usando el comando de
    Airflow compatible con la versión elegida, por ejemplo
    `airflow db migrate`.
10. Inicia los servicios y revisa sus logs.

El Webserver no debe quedar abierto a todo Internet. Restringe su puerto
a tu IP o utiliza un acceso privado. No copies los secretos en la línea
de comandos, porque pueden quedar en el historial del shell o en la
lista de procesos.

### Pruebas del maestro

-   Scheduler arranca sin errores.
-   Webserver responde desde el acceso autorizado.
-   Airflow conecta a RDS.
-   Airflow conecta a RabbitMQ.
-   El DAG aparece en la interfaz.
-   No hay secretos visibles en los logs.

No despliegues los workers todavía si estas pruebas no pasan.

------------------------------------------------------------------------

## 12. Desplegar dos workers en ECS

### Conceptos

-   **ECR:** almacena la imagen.
-   **ECS Cluster:** agrupa la capacidad lógica donde ECS gestiona las
    tareas.
-   **Task Definition:** describe la imagen, el comando, CPU/memoria,
    red, roles, logs y secretos.
-   **ECS Task:** una instancia en ejecución de la definición.
-   **Desired count:** número de tareas que queremos mantener en
    ejecución.

### Pasos

1.  Crea un ECS Cluster. Para un laboratorio puedes evaluar Fargate,
    pero revisa su costo por vCPU, memoria y duración; no asumas que es
    gratuito.
2.  Crea una Task Definition que use la imagen versionada de ECR.
3.  Configura el comando para iniciar un worker de Airflow, usando la
    sintaxis correspondiente a la versión elegida (por ejemplo,
    `airflow celery worker` en versiones que lo admitan).
4.  Define CPU y memoria suficientes para que el proceso arranque.
5.  Configura el execution role para descargar imágenes de ECR, publicar
    logs y recuperar secretos inyectados en la definición, si aplica.
6.  Configura el task role con los permisos de ejecución mínimos.
7.  Elige la VPC, las subredes y el Security Group de los workers.
8.  Permite tráfico desde el Security Group de los workers hacia RDS y
    RabbitMQ.
9.  Configura CloudWatch Logs sin imprimir secretos.
10. Ejecuta una tarea y valida que se mantenga activa y que conecte a
    RabbitMQ y RDS.
11. Escala a dos tareas y comprueba que ambas estén en estado `RUNNING`.

**Dos workers activos no garantizan que una tarea vaya a ejecutarse en
cada uno.** Airflow/Celery distribuye los trabajos según disponibilidad
y configuración. Este pipeline tiene tres tareas secuenciales y puede
que no utilice los dos workers al mismo tiempo. Los dos workers
demuestran capacidad de ejecución distribuida, no paralelismo de estas
tres tareas.

### Red de ECS

Si las tareas están en subredes privadas, necesitan conectividad a ECR y
a los servicios de logs, además de RDS y RabbitMQ. Esto puede requerir
NAT Gateway o VPC endpoints. Como estos pueden costar dinero, compara
las opciones antes de decidir; no hagas públicos los servicios de datos
para evitar configurar la red.

------------------------------------------------------------------------

## 13. Prueba de extremo a extremo

1.  Confirma que Scheduler y Webserver están sanos en EC2.
2.  Confirma que hay dos workers de ECS en ejecución.
3.  Confirma que los workers pueden conectarse al broker y a PostgreSQL.
4.  Abre la interfaz de Airflow desde una conexión autorizada.
5.  Activa el DAG `pipeline_sencillo_aws`.
6.  Ejecuta el DAG manualmente.
7.  Abre los logs de cada tarea.
8.  Verifica que la tarea 1 imprime la hora inicial, la tarea 2 imprime
    el mensaje y espera dos segundos, y la tarea 3 imprime la hora
    final.
9.  Verifica que las tres tareas quedan en estado `success`.
10. Confirma que no se imprimen contraseñas, tokens ni claves.

El pipeline no necesita una tabla de negocio. RDS se utiliza para los
metadatos internos de Airflow; los mensajes de las tareas se consultan
en los logs.

------------------------------------------------------------------------

## 14. Diagnóstico de errores

### Las tareas quedan en cola (`queued`)

-   Verifica que las tareas worker de ECS siguen en `RUNNING`.
-   Revisa los logs del worker.
-   Comprueba que el worker y el Scheduler usan el mismo broker, base de
    datos y claves.
-   Verifica la conectividad al endpoint de RabbitMQ y que las
    credenciales son correctas.
-   Comprueba que el worker usa la misma versión compatible de Airflow y
    contiene el DAG.

### ECS se detiene inmediatamente

-   Revisa el motivo de detención en ECS.
-   Revisa CloudWatch Logs.
-   Verifica el comando de arranque, la imagen y el puerto/red.
-   Revisa memoria y CPU.
-   Verifica que los secretos existen y que el execution role tiene
    permiso para leerlos.
-   Asegúrate de que el proceso principal del contenedor es el worker y
    que no termina después de ejecutar un comando de diagnóstico.

### Airflow no conecta a RDS

-   Revisa DNS, rutas, Security Groups, puerto `5432`, usuario,
    contraseña, base de datos y TLS.
-   Confirma que el acceso público está desactivado y que el tráfico
    privado está permitido desde los Security Groups correctos.

### Airflow no conecta a RabbitMQ

-   Usa el endpoint exacto de Amazon MQ.
-   Verifica el puerto TLS y el certificado.
-   Comprueba el Security Group y las credenciales.
-   No confundas el puerto AMQPS con el de la interfaz de
    administración.

### El DAG no aparece

-   Confirma que el archivo está en la ruta configurada de DAGs.
-   Revisa los logs del Scheduler.
-   Valida los imports y la compatibilidad de la versión de Airflow.
-   Confirma que el archivo está presente en la imagen que ejecutan los
    componentes necesarios.

------------------------------------------------------------------------

## 15. Seguridad mínima obligatoria

-   No uses el usuario raíz para tareas rutinarias.
-   Activa MFA.
-   Usa roles IAM en EC2 y ECS en lugar de claves AWS estáticas.
-   Limita permisos a los recursos y secretos necesarios.
-   Mantén RDS y RabbitMQ en redes privadas.
-   No abras SSH, Airflow, PostgreSQL o RabbitMQ a todo Internet.
-   Usa TLS donde corresponda.
-   No subas `.env`, contraseñas ni claves al repositorio.
-   No escribas secretos en logs.
-   Mantén la imagen y dependencias actualizadas y versionadas.
-   No compartas la interfaz de Airflow sin autenticación y controles de
    acceso adecuados.

------------------------------------------------------------------------

## 16. Control de costos y limpieza

El costo depende de las regiones, tipos de instancia, duración,
almacenamiento, transferencia, logs, secretos y opciones de alta
disponibilidad. Antes de crear cada servicio, revisa su precio en
`us-east-2`.

Al terminar la sesión de aprendizaje:

1.  Detén el DAG y espera a que no haya tareas activas.
2.  Reduce a cero el número de workers de ECS o detén las tareas.
3.  Detén o elimina EC2 si no la necesitas; recuerda que detener una
    instancia no elimina necesariamente el costo de discos EBS, IPs u
    otros recursos asociados.
4.  Elimina RDS cuando ya no necesites sus datos, revisando
    cuidadosamente snapshots y retención.
5.  Elimina el broker de Amazon MQ si no vas a seguir usándolo.
6.  Revisa NAT Gateways, Elastic IPs, volúmenes EBS, snapshots,
    endpoints VPC y logs.
7.  Decide si conservarás ECR y Secrets Manager; ambos pueden generar
    costos.
8.  Comprueba la consola de facturación en los días siguientes.

No borres datos o recursos sin confirmar que no los necesitas. Eliminar
un recurso puede ser irreversible; los snapshots y backups también
pueden costar dinero.

### Costos que no debes pasar por alto

-   EC2 y almacenamiento EBS.
-   RDS mientras la instancia está aprovisionada, más almacenamiento y
    backups.
-   Amazon MQ mientras el broker está aprovisionado.
-   Fargate/ECS por recursos y tiempo de ejecución, según el modo
    elegido.
-   Secrets Manager por secretos y llamadas aplicables.
-   ECR por almacenamiento y transferencia.
-   CloudWatch por ingesta y retención de logs.
-   NAT Gateway, IPs públicas, endpoints VPC y transferencia de datos.

El presupuesto de USD 5 es un aviso, no una promesa de que el despliegue
completo quepa en esa cantidad. Para minimizar costos, construye la
imagen y prueba el DAG localmente primero, crea los servicios
administrados solo cuando estés listo para la integración, y elimina los
recursos cuando termines.

------------------------------------------------------------------------

## 17. Checklist final

### Cuenta y red

-   [ ] Región `us-east-2`.
-   [ ] MFA activado.
-   [ ] Plan/créditos revisados.
-   [ ] Presupuesto y alertas configurados.
-   [ ] VPC, subredes, rutas y Security Groups revisados.

### Secretos y datos

-   [ ] Secrets Manager configurado.
-   [ ] Roles IAM con privilegio mínimo.
-   [ ] RDS privado y accesible solo desde los componentes necesarios.
-   [ ] Amazon MQ privado, con TLS y credenciales protegidas.

### Airflow y contenedores

-   [ ] DAG probado localmente.
-   [ ] Versión de Airflow fijada y compatible.
-   [ ] Imagen construida y subida a ECR.
-   [ ] Maestro de Airflow activo en EC2.
-   [ ] Dos workers activos en ECS.
-   [ ] Scheduler, workers, RabbitMQ y RDS conectados.

### Validación y limpieza

-   [ ] Las tres tareas terminan en `success`.
-   [ ] Se ve el mensaje de inicio y final con fecha/hora.
-   [ ] La tarea 2 espera dos segundos.
-   [ ] No se exponen secretos en logs.
-   [ ] Recursos y costos revisados después de las pruebas.

------------------------------------------------------------------------

## 18. Documentación oficial para consultar

-   AWS Management Console: https://console.aws.amazon.com/
-   Billing and Cost Management:
    https://console.aws.amazon.com/costmanagement/
-   Amazon EC2: https://docs.aws.amazon.com/ec2/
-   Amazon RDS for PostgreSQL:
    https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_PostgreSQL.html
-   Amazon MQ: https://docs.aws.amazon.com/amazon-mq/
-   Amazon ECS: https://docs.aws.amazon.com/ecs/
-   Amazon ECR:
    https://docs.aws.amazon.com/AmazonECR/latest/userguide/what-is-ecr.html
-   AWS Secrets Manager: https://docs.aws.amazon.com/secretsmanager/
-   AWS IAM:
    https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html
-   Apache Airflow: https://airflow.apache.org/docs/
-   Airflow CeleryExecutor:
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/celery.html
-   Docker Compose: https://docs.docker.com/compose/

**Recomendación de trabajo:** avanza fase por fase. No despliegues EC2,
RDS, Amazon MQ y ECS a la vez. Primero valida la cuenta y el
presupuesto; después crea la red y los secretos; luego comprueba RDS y
RabbitMQ; finalmente despliega Airflow y los workers. Así cada error
tendrá un lugar concreto donde buscarlo.
