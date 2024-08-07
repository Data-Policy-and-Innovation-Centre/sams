import sys
import os
from boxsdk import Client, OAuth2

def authenticate_box():
    auth = OAuth2(
        client_id=os.environ['BOX_CLIENT_ID'],
        client_secret=os.environ['BOX_CLIENT_SECRET'],
        access_token=os.environ['BOX_ACCESS_TOKEN']
    )
    client = Client(auth)
    return client

def download_file_from_box(client, file_id, download_path):
    box_file = client.file(file_id).get()
    with open(download_path, 'wb') as output_file:
        box_file.download_to(output_file)
    print(f"Downloaded file {box_file.name} to {download_path}")

def main():
    file_id = sys.argv[1]
    download_path = sys.argv[2]

    client = authenticate_box()
    download_file_from_box(client, file_id, download_path)

if __name__ == '__main__':
    main()
