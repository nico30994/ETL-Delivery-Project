## ETL Delivery Project
## Resumen
Se necesita generar una BBDD de un proyecto de entregas individuales a 10.000 domicilios para finalmente actualizarla (bajas, altas y modificaciones) de forma automática según la informacion brindada por dos proveedores de logística para generar 1 excel semanal y reportes en vivo.

## Persistencia
Un dataset del `proveedor A -> P1` se actualiza en cualquier momento en un Google Sheet(Snapshot).
Un dataset del `proveedor B -> P2` se recibe en excel todos los días(Snapshot).


## Objetivos (Generarlos de forma automática)
* Se debe verificar la información de los datasets.
* Se deben transformar los datasets
* Modificar los registros de la BBDD con (`Estado, fecha de entrega, numero de remito, etc.`).
* Generar informe semanal de entregas
* Generar informe diario con KPIs
* Generar informe de Tableau

## Prerequisitos
* PostgreSQL

## Desarrollo local
Las tablas se generan con `sql/db.sql`
Los domicilios nuevos que se van recibiendo, se dan de alta en la BBDD con `registros_bbdd/add_roster.py`.

* El P1 como carga sus datos en un google drive se ejecuta `main/etl_p1.py` para extraer la información.
* El P2 envia la informacion por mail asi que simplemente se descarga

![EjProv](https://github.com/nico30994/ETL-Delivery-Project/blob/main/imgs/add_reg.jpg)

Teniendo ambos archivos, se ejecuta `main/etl_main.py` para extraer, transformar y cargar en un solo archivo, este archivos esta listo para enviar a las personas que lo solicitan y ser leidas desde tableau para generar los reportes y sus correspondientes toma de decisiones.

Con `registros_bbdd/add_status.py` se cargan los estados a la BBDD.

Tableau accede a la base de datos y actualiza automaticamente los reportes:

![ResultadoTableau](https://github.com/nico30994/ETL-Delivery-Project/blob/main/imgs/Roster.jpg)
![ResultadoTableau2](https://github.com/nico30994/ETL-Delivery-Project/blob/main/imgs/info.jpg)

Mejoras futuras:
* Automatizar tareas con Airflow
* Agregar Docker