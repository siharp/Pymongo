from pymongo import MongoClient
from datetime import datetime
from faker import Faker

fake = Faker()

# function connect to database 
def connect_database(uri: str, database_name:str) -> str:
    client = MongoClient(uri)
    database = client[database_name]
    return database 

# function to insert data
def insert_data(database:str, colection_name:str, data):

    collection = database[colection_name]

    current_time = datetime.now()

    if isinstance(data, list):
        for i in data:
            i['inserted_at']= current_time
        result = collection.insert_many(data)
        return result.inserted_ids
    
    elif isinstance(data, dict):
        data['inserted_at'] = current_time

        result = collection.insert_one(data)
        return result.inserted_id
    
    else:
        raise ValueError('data harus berupa dict atau list of dictionary')
    
def generate_fake_data(count_data:int) -> list:
    
    data = []
    for i in range(count_data):
       fake_user = {
            'name': fake.name(),
            'age': fake.random_int(min=12, max=67),
            'city': fake.city()
        }
       data.append(fake_user)
       
    return data


if __name__ == '__main__':

    mongo_uri = 'mongodb://root:password@localhost:27017'

    database_name = 'belajar'

    single_data = {
        "name": "Alice",
        "age": 30,
        "city": "Jakarta"
    }

    multiple_data = [
        {"name": "Bob", "age": 25, "city": "Bandung"},
        {"name": "Charlie", "age": 35, "city": "Surabaya"}
    ]

    # generate fake data


    try:
        # connect to database
        db = connect_database(mongo_uri, database_name=database_name)

        # # insert single data
        # inserted_id = insert_data(db, 'test', single_data)
        # print(f"Data inserted with ID: {inserted_id}")

        # Insert multiple data
        fake_data = generate_fake_data(1000)
        inserted_ids = insert_data(db, "test", fake_data)
        print(f"Multiple data inserted with IDs: {inserted_ids}")


    except Exception as e:
        print(f'Eror connect to datababase because {e}')