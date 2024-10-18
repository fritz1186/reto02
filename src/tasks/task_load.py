from config import config
from mysql import connector
from prefect import task

@task (name="Carga de data en DB")
def task_load_empresa(empresa_data):
    with connector.connect(**config.MYSQL_CONFIG) as db:
        try:

            with db.cursor() as cursor:
                query_insert = """
                    insert into empresa (ruc, nombre_o_razon_social, direccion, estado, condicion, ubigeo_sunat, celular)
                    values (%s,%s,%s,%s,%s,%s,%s)
                """
                cursor.execute(query_insert, empresa_data)
                db.commit()
        except Exception as error:
            print(f"Error : {error}")
            