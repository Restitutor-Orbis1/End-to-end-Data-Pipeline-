import uuid
from cassandra.cluster import Cluster

def create_test_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect('spark_streaming')  # Make sure 'spark_streaming' keyspace exists
        return session
    except Exception as e:
        print(f"Couldn't connect to Cassandra: {e}")
        return None

def test_insert_data(session):
    user_id = uuid.uuid4()  # Generate a new UUID
    first_name = 'Test'
    last_name = 'User'
    
    try:
        session.execute("""
            INSERT INTO created_users (id, first_name, last_name) 
            VALUES (%s, %s, %s)
        """, (user_id, first_name, last_name))
        print(f"Data inserted for user: {first_name} {last_name}")
    except Exception as e:
        print(f"Couldn't insert data due to error: {e}")

# Main
session = create_test_cassandra_connection()
if session:
    test_insert_data(session)