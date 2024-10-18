from prefect import flow
from tasks.task_load import task_load_empresa
from tasks.task_extract import task_extract_csv, task_extract_ruc
from tasks.task_utils import task_init_table, task_get_empresa_from_db
from tasks.utils import validate_ruc, handle_invalid_ruc


BASELINE_TASK = True
DATA_PATH = "./resources/empresas.csv"

@flow (name="RETO-ETL-RUC")
def main_flow():

    #Inicializa la tabla
    if BASELINE_TASK:
        task_init_table()
    
    #Extra datos del archivo "empresas.csv" a una Lista
    initial_empresa_data = task_extract_csv(DATA_PATH)

    #Procesa casa elemento de la Lista Extraida
    for empresa in initial_empresa_data:
        #Obtiene el Ruc y el Celular de cada fila
        ruc = empresa[0]
        celular = empresa[1]

        #Verifica si el RUC es valido
        if validate_ruc (ruc):
            #verifica si ya existe la empresa en la Base de Datos
            empresa_exists = task_get_empresa_from_db(ruc)
            #Sino existe la empresa procede a crearla
            if not empresa_exists:
                #Obtiene informacion de la empresa consultando a la API
                api_empresa_data = task_extract_ruc(ruc)

                #Si se obtiene datoa de la empresa se registra la empresa
                if api_empresa_data:
                    #Configura los datos de la Empresa
                    empresa_data = (ruc, *api_empresa_data, celular)
                    print (ruc)
                    print (api_empresa_data)
                    task_load_empresa(empresa_data)                
        else:
            handle_invalid_ruc(ruc)                


if __name__ == "__main__":
    main_flow()