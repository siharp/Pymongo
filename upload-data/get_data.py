from pymongo import MongoClient
import json
from datetime import datetime
import os 

# Fungsi koneksi ke database
def connect(uri, database_name):
    try:
        client = MongoClient(uri)
        database = client[database_name]
        return database
    except Exception as e:
        print(f"Koneksi gagal: {e}")
        return None

# Fungsi pengambilan data
def get_data(database, collection_name):
    try:
        collection = database[collection_name]
        result = collection.find()
        hasil = []
        for i in result:
            hasil.append(i)
        return hasil
    except Exception as e:
        print(f"Pengambilan data gagal: {e}")
        return []

# Fungsi penyimpanan data ke file
def save_file(data):
    try:
        current_dir = os.getcwd()
        folder_path = os.path.join(current_dir, 'result')
        date = datetime.now().strftime("%Y-%m-%d_%H")
        file_path = os.path.join(folder_path, f"{date}.json")
        with open(f"{file_path}", "w") as file:
            json.dump(data, file, indent=4, default=str)
    except Exception as e:
        print(f"Penyimpanan data gagal: {e}")

if __name__ == "__main__":
    uri = "mongodb://root:password@localhost:27017"
    database_name = "belajar"
    collection_name = "test"

    db = connect(uri, database_name)
    if db is not None:
        data = get_data(db, collection_name)
        save_file(data)
        print('Data berhasil disimpan')
    else:
        print("Koneksi database gagal.")

