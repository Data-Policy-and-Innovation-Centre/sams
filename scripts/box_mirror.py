from boxsdk import Client, OAuth2
import os
import zipfile

def authenticate_box():
    auth = OAuth2(
        client_id=os.environ['BOX_CLIENT_ID'],
        client_secret=os.environ['BOX_CLIENT_SECRET'],
        access_token=os.environ['BOX_DEVELOPER_TOKEN']
    )
    client = Client(auth)
    return client

def get_script_directory():
    script_path = os.path.abspath(__file__)
    script_directory = os.path.dirname(script_path)
    return script_directory

def get_parent_directory():
    parent_directory = os.path.dirname(get_script_directory())
    return parent_directory

def zip_folder(folder_path, output_zip):
    """Create a ZIP file of the specified folder."""
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Walk through the folder and add files to the ZIP
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                # Add file to the ZIP archive
                zipf.write(file_path, os.path.relpath(file_path, folder_path))
    print(f"Folder {folder_path} has been zipped to {output_zip}")

def check_file_exists(client, folder_id, file_name):
    """Check if a file with the given name exists in the specified folder."""
    folder = client.folder(folder_id).get()
    items = folder.get_items()
    for item in items:
        if item.name == file_name and item.type == 'file':
            return item
    return None

def delete_file(client, file_id):
    """Delete the file with the given ID."""
    try:
        client.file(file_id).delete()
        print(f"Deleted file with ID {file_id}")
    except BoxException as e:
        print(f"An error occurred while deleting the file: {e}")

def upload_file_to_box(client, folder_id, file_path):
    """Upload a file to Box."""
    file_name = os.path.basename(file_path)
    try:
        # Check if a file with the same name exists
        existing_file = check_file_exists(client, folder_id, file_name)
        if existing_file:
            print(f"File '{file_name}' already exists. Deleting the file.")
            # Delete the existing file
            delete_file(client, existing_file.id)
        
        # Upload the new file
        with open(file_path, 'rb') as file_stream:
            uploaded_file = client.folder(folder_id).upload_stream(file_stream, file_name)
        print(f"Uploaded new file '{uploaded_file.name}' with ID {uploaded_file.id}")
    except BoxException as e:
        print(f"An error occurred: {e}")
        
def main():
    folder_id = '279109297300'  # Replace with your Box folder ID
    local_folder_path = get_parent_directory()  # Folder to zip and upload
    zip_file_path = os.path.join(get_script_directory(), 'skills_project.zip')  # Output ZIP file path

    # Create ZIP file
    zip_folder(local_folder_path, zip_file_path)

    # Authenticate and upload the ZIP file to Box
    client = authenticate_box()
    upload_file_to_box(client, folder_id, zip_file_path)

    # Remove zip
    os.remove(zip_file_path)

if __name__ == '__main__':
    main()
