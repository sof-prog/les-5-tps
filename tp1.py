import os
import sys
from urllib.request import urlretrieve
from minio import Minio
from minio.error import S3Error

def download_data():
    """
    Télécharge les fichiers de données depuis les URLs spécifiées et les enregistre dans le dossier 'data/raw'.
    """
    urls = [
        'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-11.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-12.parquet',
    ]
    save_dir = "../../data/raw"
    os.makedirs(save_dir, exist_ok=True)

    for url in urls:
        file_name = os.path.basename(url)
        save_path = os.path.join(save_dir, file_name)
        print(f"Téléchargement de {file_name}...")

        try:
            urlretrieve(url, save_path)
            print(f"{file_name} téléchargé avec succès et enregistré dans {save_path}.")
        except Exception as e:
            print(f"Échec du téléchargement de {file_name}. Erreur : {e}")

def upload_to_minio():
    """
    Téléverse les fichiers Parquet depuis le dossier 'data/raw' vers un bucket Minio.
    """
    minio_client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    bucket_name = "atl-datamart-project"

    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        print(f"Bucket {atl-datamart-project} créé.")
    else:
        print(f"Bucket {atl-datamart-project} existe déjà.")

    data_dir = "../../data/raw"
    for file_name in os.listdir(data_dir):
        if file_name.endswith(".parquet"):
            file_path = os.path.join(data_dir, file_name)
            print(f"Téléversement de {file_name} dans {bucket_name}...")

            try:
                with open(file_path, "rb") as file_data:
                    file_stat = os.stat(file_path)
                    minio_client.put_object(
                        bucket_name,
                        file_name,
                        file_data,
                        file_stat.st_size
                    )
                print(f"{file_name} téléversé avec succès dans {bucket_name}.")
            except S3Error as e:
                print(f"Erreur Minio lors de l'accès au bucket {bucket_name} : {e}")
            except Exception as e:
                print(f"Erreur inattendue : {e}")

def main():
    download_data()
    upload_to_minio()

if __name__ == '__main__':
    sys.exit(main())