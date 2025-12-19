import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import sys
from pathlib import Path
import threading


db_lock = threading.Lock()
cwd = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(cwd, 'logs.txt')
try:
    with open(log_file):
        pass
except FileNotFoundError:
    os.makedirs(log_file, exist_ok=True)
duplicates_file = os.path.join(cwd, 'duplicates.txt')

database_file = os.path.join(cwd, 'database.json')
try:
    with open(database_file, 'r') as f:
        pass
except FileNotFoundError:
    logging.error('database.json not found.\nCreating database.json')
    database = {

    }
    os.makedirs(os.path.dirname(database_file), exist_ok=True)
    with open(database_file, 'w') as f:
        json.dump(database, f, indent=4)
except json.JSONDecodeError:
    logging.error('Error decoding database.json.')
    exit(1)

try:
    with open(duplicates_file, 'r') as f:
        pass
except FileNotFoundError:
    os.makedirs(duplicates_file, exist_ok=True)
settings_file = os.path.join(cwd, 'settings.json')
try:
    try:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, mode='w', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        logging.info('Logging setup complete.')
        logging.info(f'Logs will be written to: {log_file}')
    except Exception as e:
        print(f'Exception occurred while setting up logging: {e}')
        exit(1)


    try:
        with open(settings_file, 'r') as f:
            settings = json.load(f)
            directory_path = settings.get('directory', '')
    except FileNotFoundError:
        logging.error('settings.json not found.')
        settings = {
        "directory": "C:\\example",
        "recursive": True
        }
        os.makedirs(os.path.dirname(settings_file), exist_ok=True)
        with open(settings_file, 'w') as f:
            json.dump(settings, f, indent=4)
    except json.JSONDecodeError:
        logging.error('Error decoding settings.json.')
        exit(1)




    try:
        with open(database_file, 'r') as f:
            hash_database = json.load(f)
    except json.JSONDecodeError:
        logging.error('Error decoding database.json')
        hash_database = {}

    if not directory_path:
        logging.error('No directory path found in settings.json.')
    else:
        logging.info(f'Directory to scan: {directory_path}')

        def hash_file(filename):
            '''Generate SHA-256 hash of a file.'''
            h = hashlib.sha256()
            with open(filename, 'rb') as file:
                while chunk := file.read(8192):
                    h.update(chunk)
            return h.hexdigest()

        def compare_files(directory):
            '''Compare files by their hashes.'''
            duplicates = []

            def process_file(file):
                logging.info(f'Processing file: {file}')
                file_path = Path(file).resolve()
                file_hash = hash_file(str(file_path))
                file_entry = {
                    'path': str(file_path)
                }
                unique_key = file_hash
                with db_lock:
                    if unique_key in hash_database:
                        original_file = hash_database[unique_key]['path']
                        original_path = Path(original_file).resolve()
                        if not original_path.is_file():
                            logging.info(f'File {str(original_path)} does not exist, marking new found file as original')
                            hash_database[unique_key] = file_entry    
                            return
                        if original_path != file_path:
                            logging.info(f'Duplicate found: {str(file_path)} is a duplicate of {str(original_path)}')
                            if file_path.is_file():
                                file_path.unlink()
                            with open(duplicates_file, 'a') as f:
                                f.write(f'Duplicate deleted: {str(file_path)} which was a duplicate of {str(original_path)}\n')
                                duplicates.append((str(file_path), str(original_path)))
                    else:
                        hash_database[unique_key] = file_entry
                        logging.info(f'Finished processing file: {str(file)}')
                
            with ThreadPoolExecutor() as executor:
                recursive = settings.get('recursive', True)
                if recursive:
                    logging.info('Scanning directory recursively...')
                    file_paths = (p for p in Path(directory).rglob('*') if p.is_file())
                    executor.map(process_file, file_paths)
                else:
                    logging.info('Scanning directory non-recursively...')
                    file_paths = (p for p in Path(directory).glob('*') if p.is_file())
                    executor.map(process_file, file_paths)
            return duplicates

        duplicates = compare_files(directory_path)

        if duplicates:
            logging.info('\nDuplicate files found:')
            for duplicate, original in duplicates:
                logging.info(f'{duplicate} is a duplicate of {original}')
        else:
            logging.info('\nNo duplicate files found.')

        try:
            with open(database_file, 'w') as f:
                json.dump(hash_database, f, indent=4)
        except IOError as e:
            logging.error(f'IOError: Unable to write to {database_file}. {e}')

except KeyboardInterrupt:
    logging.warning('Emergency stop triggered. Exiting the program...')
    sys.exit(0)
