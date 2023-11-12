from qdrant_client.http.models import Batch
import Infrastucture.AiDevsKeys as Keys
from qdrant_client.http import models
from qdrant_client import QdrantClient

vector_dimension = 1536
qdrant_url = "localhost:6333"


def collection_exists(collection_name: str, client=QdrantClient(url=qdrant_url, timeout=100)) -> bool:
    collections = client.get_collections().collections
    return any(collection.name == collection_name for collection in collections)


def create_collection(collection_name: str, vector_dimension: int, client=QdrantClient(url=qdrant_url, timeout=100)):
    client.create_collection(collection_name=collection_name,
                             vectors_config=models.VectorParams(size=vector_dimension, distance=models.Distance.COSINE),
                             on_disk_payload=True)
    print(f"Collection '{collection_name}' created successfully.")


def check_and_create_collection(collection_name: str, vector_dimension: int, client=QdrantClient(url=qdrant_url, timeout=100)):
    if not collection_exists(client, collection_name):
        create_collection(client, collection_name, vector_dimension)
        print(f"Collection '{collection_name}' has been created.")
    else:
        print(f"Collection '{collection_name}' already exists.")


def check_collection_size(collection_name: str, client=QdrantClient(url=qdrant_url, timeout=100)) -> int:
    if collection_exists(client=client, collection_name=collection_name):
        collection = client.get_collection(collection_name=collection_name)
        print(collection)
        return collection.vectors_count
    else:
        return -1;


def upsert_to_collection(collection_name: str, data, client=QdrantClient(url=qdrant_url, timeout=100)):
    check_and_create_collection(client=client, collection_name=collection_name, vector_dimension=vector_dimension)
    if check_collection_size(client=client, collection_name=collection_name) > 299:
        print("collection > 299, skip")
    else:
        print("upsert start")
        ids = [x['id'] for x in data]
        print(ids)
        vectors = [x['embedding'] for x in data]
        print(vectors)
        client.upsert(collection_name=collection_name, points=Batch(ids=ids, vectors=vectors))

    print("upsert end")
    collection = client.get_collection(collection_name=collection_name)
    print(collection)


def search_data(collection_name: str, vector_search, client=QdrantClient(url=qdrant_url, timeout=100)):
    result = client.search(collection_name=collection_name, query_vector=vector_search, limit=1)
    return result
