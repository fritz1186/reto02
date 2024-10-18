import csv
import requests
from prefect import task
from config import config
from tasks.utils import handle_invalid_ruc

@task (name="Extraer RUC del archivo 'empresa.csv'")
def task_extract_csv(filename):
    #Inicializa Lista donde se guardara los datos extraidos del archivo .csv
    data =[]
    #Lee el archivo .csv
    with open (filename, "r") as csv_file:
        #Lee los datos del archivo .csv y los extrae a un Diccionario
        csv_reader = csv.DictReader (csv_file)
        #Procesa cada fila del Dictionario obtenido
        for row in csv_reader:
            #Guarda el RUC y el Celular en ua tupla
            tmp_data=(row["ruc"], row["celular"])
            #La tupla se agrega a la Lista 'data'
            data.append (tmp_data)
    #Se devuelve una Lista con los RUC y celulares extraidos del archivo .csv
    return data


@task (name="Extraer data del RUC")
def task_extract_ruc(ruc):

    #Establece la URL para RUC
    url_ruc = config.ENDPOINT["ruc"]

    #Obtiene el token Bearer de autorizacion de la clase Config
    token = config.API_TOKEN

    #Configura los headers y la data del request
    headers = {
        "Authorization" : "Bearer " + token,    
        "Content-Type" : "application/json"
    }

    data = {
        "ruc" : ruc
    }   

    #Se ejecuta el request
    response = requests.post (url_ruc, json=data, headers=headers)

    #Procesa el response
    if response.status_code ==200:
        if response.json()["success"]:
            #Guarda en una Lista los datos del ruc del json obtenido en el response
            data = response.json()["data"]
            #Guarda cada dato en una variable
            nombre_o_razon_social = data["nombre_o_razon_social"] 
            direccion = data["direccion"]
            estado = data["estado"]
            condicion = data["condicion"]
            ubigeo_sunat = data["ubigeo_sunat"]

            return (nombre_o_razon_social, direccion, estado, condicion, ubigeo_sunat)
        else:
            #En caso que no encuentre el RUC se muestra un mensaje y se guarda el ruc en un archivo
            print (ruc)
            print("No se encontro")
            handle_invalid_ruc(ruc)            
    else:
        print ("error: ", response.status_code)