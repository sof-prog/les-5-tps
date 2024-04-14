from minio import Minio
from minio.error import S3Error
import pandas as pd
from io import BytesIO
from sqlalchemy import create_engine
import pyarrow.parquet as pq
# Configuration Minio
minio_client = Minio(
"127.0.0.1:9000",
access_key="admin",
secret_key="password",
Un script Python se charge de récupérer les données stockées dans Minio et de
les charger dans PostgreSQL pour stockage initial.

secure=False
)
# Configuration PostgreSQL
db_params = {
'host': 'localhost',
'port': 5432,
'user': 'postgres',
'password': 'idir',
'database': 'TaxiYolow'
}
# Nom du seau (bucket) Minio et liste des objets à télécharger
bucket_name = "datamartbukuts"
object_names = ["yellow_tripdata_2023-06.parquet", "yellow_tripdata_2023-07.parquet",
"yellow_tripdata_2023-08.parquet", "yellow_tripdata_2023-09.parquet"]
# Fonction pour télécharger un objet depuis Minio
def download_object(bucket, object_name):
try:
response = minio_client.get_object(bucket, object_name)
return response.read()
except S3Error as e:
print(f"Erreur lors du téléchargement de l'objet {object_name}: {e}")
return None
# Boucle pour télécharger chaque objet depuis Minio
for object_name in object_names:
data_bytes = download_object(bucket_name, object_name)
# Charger les données avec PyArrow
if data_bytes:
table = pq.read_table(BytesIO(data_bytes))
df = table.to_pandas()
# Créer une connexion à la base de données PostgreSQL
engine = create_engine(f'postgresql+psycopg2://{db_params["user"]}:
{db_params["password"]}@{db_params["host"]}:
{db_params["port"]}/{db_params["database"]}')
# Insérer le DataFrame dans la base de données
try:
table_name = object_name.replace('.parquet', '').replace('.', '_')
df.to_sql(table_name, engine, if_exists="replace", index=False)
print(f"Données du fichier {object_name} chargées dans la base de données avec succès.")
except Exception as e:
print(f"Erreur lors de l'insertion des données du fichier {object_name} dans la base de
données : {e}")
finally:
engine.dispose() # Fermer la connexion