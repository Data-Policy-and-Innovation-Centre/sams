import sys
import os
import box_mirror
from boxsdk import Client, JWTAuth

def authenticate_box(config_path):
    """
    Authenticate with Box using a JSON Web Token (JWT) and return a client object.

    Args:
        config_path (str): The path to the configuration file containing the JWT settings.

    Returns:
        Client: A Box client object authenticated with the provided JWT settings.
    """
    # Authenticate using JWT
    auth = JWTAuth.from_settings_file(config_path)
    client = Client(auth)

    # Make API calls
    user = client.user().get()
    print(f"Authenticated as: {user.name}")
    return client


def download_file_from_box(client, file_id, download_path):
    """
    Downloads a file from Box using the provided client, file ID, and download path.

    Args:
        client (boxsdk.Client): The Box client used to authenticate and make API calls.
        file_id (str): The ID of the file to download.
        download_path (str): The path where the downloaded file will be saved.

    Returns:
        None

    Raises:
        None

    Example:
        client = boxsdk.Client('your_access_token')
        download_file_from_box(client, '1234567890', 'path/to/download/file.txt')
        # Downloads the file with ID '1234567890' and saves it to 'path/to/download/file.txt'
    """
    box_file = client.file(file_id).get()
    with open(download_path, 'wb') as output_file:
        box_file.download_to(output_file)
    print(f"Downloaded file {box_file.name} to {download_path}")

def main():
    root_dir = box_mirror.get_parent_directory()
    jwt_config_path = os.path.join(root_dir, 'config.json')
    data_config_path = os.path.join(root_dir,'fileids.csv')

    client = authenticate_box(config_path)
    download_file_from_box(client, file_id, download_path)

if __name__ == '__main__':
    main()
