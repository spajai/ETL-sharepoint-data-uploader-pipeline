#!/usr/bin/python3
import json, os, sys
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
import settings

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
USERNAME = settings.USER
PASSWORD = settings.PASSWORD

def upload_file(local_file_path, output_sub_dir):
    credential_store = UserCredential(USERNAME, PASSWORD)
    ctx = ClientContext(settings.SHAREPOINT_URL).with_credentials(credential_store)
    remote_full_path = output_sub_dir +  '/' + settings.OUTPUT_PATH

    print("[INFO] uploading file ", local_file_path , " To Remote path ", remote_full_path)

    target_folder = ctx.web.get_folder_by_server_relative_url(settings.OUTPUT_PATH)
    size_chunk = 1000000
    file_size = os.path.getsize(local_file_path)
    uploaded_file = target_folder.files.create_upload_session(local_file_path, size_chunk).execute_query()

    print('File {0} has been uploaded successfully'.format(uploaded_file.serverRelativeUrl))

upload_file()