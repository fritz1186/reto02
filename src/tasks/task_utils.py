from prefect import task
from mysql import connector
from config import config

@task(name="Inicializar la tabla de de Usuario")
def task_init_table():
    try:
        with connector.connect(**config.MYSQL_CONFIG) as db:
            with db.cursor() as cursor:
                #Drop table if exists
                query_drop_table = "drop table if exists empresa"
                cursor.execute(query_drop_table)
                db.commit()

                #Create table Empresa
                query_create_table = """
                    create table if not exists empresa (
                        id int auto_increment primary key,
                        ruc varchar(11),
                        nombre_o_razon_social varchar(255),
                        direccion varchar(255),
                        estado varchar(100),
                        condicion varchar(100),
                        ubigeo_sunat varchar(6),
                        celular varchar (20)
                    )
                """
                cursor.execute(query_create_table)
                db.commit()
    except Exception as error:
        print ("Error al crear Tabla: ", error)


@task (name="Consultar si ya existe la empresa en la db")
def task_get_empresa_from_db(ruc):
    with connector.connect(**config.MYSQL_CONFIG) as db:
        with db.cursor() as cursor:
            #Query
            query_select_one = "select * from empresa where ruc = %s"
            #Ejecuta el cursor con el query configurado
            cursor.execute(query_select_one, (ruc,))
            #Obtiene la fila que resulta del query
            empresa = cursor.fetchone()
            #Devuelve la fila del Ruc
            return empresa           