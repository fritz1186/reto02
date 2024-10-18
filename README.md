# ETL  de RUCs utilizando ApiPeruDev
Implementacion de un ETL para RUCs de empresas. Los datos de: el número de ruc y sus ventas anuales se extraen de un archivo `empresas.csv`. Luego estos datos son complementados con la informacion de razon social y domicilio fiscal de la empresa. Estos nuevos datos son obtenidos a través de ApiPeruDev.
Finalmente los datos se guardan en una bd.

## API
Se uso la API 'https://apiperu.dev/api/ruc' para consultar los datos de los RUCs almacenados en el archivo"empresas.csv", estos RUC se consultaron en la API y la informacion obtenida se guardo en la tabla Empresa de la bd db_codigo

