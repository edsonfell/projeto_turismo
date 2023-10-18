import functions_framework
import requests
from google.cloud import storage


@functions_framework.http
def hello_http(request):
    print('Iniciando processo de download...')
    request_json = request.get_json(silent=True)
    print(f"Parametros recebidos: {request_json}")
    bucket_name = "turismo_bucket"

    print("Inicializando o cliente de armazenamento do Google Cloud")
    storage_client = storage.Client()
    # Obtém o bucket
    print("Criando client do bucket")
    bucket = storage_client.bucket(bucket_name)
    for csv_url in request_json['csv_to_download']:
        file_name = csv_url.split("/")[-1]
        try:
            print(f"Iniciando download do arquivo {file_name}")
            response = requests.get(csv_url)
            if response.status_code == 200:
                # Cria um objeto de blob no bucket
                blob = bucket.blob(file_name)

                # Faz o upload do conteúdo do arquivo para o blob
                blob.upload_from_string(response.text, content_type='text/csv')

                print(f"Arquivo CSV baixado e salvo em gs://{bucket_name}/{file_name}")
            else:
                print("Falha ao baixar o arquivo CSV!")

        except Exception as e:
            print(f"Erro: {e}")
    return f'Finalizado!'
