from redis_queue import Consumer
from redis import Redis
from neo4j_birtix_db_repo.repos import ElapsedTimeRepository
from neo4j_birtix_db_repo.models import ElapsedTimePayload
from neo4j import GraphDatabase

URI = "bolt://mirror-db:7687"
AUTH = ("neo4j", "password")
redis = Redis('queue', 6379, decode_responses=True)
driver = GraphDatabase.driver(URI, auth=AUTH)


class ElapsedItemConsumer(Consumer):
    def __init__(self, elapsed_items_repo: ElapsedTimeRepository, redis, stream_key, group_name, consumer, buffer_size, fulsh_time):
        super().__init__(redis, stream_key, group_name, consumer, buffer_size, fulsh_time)
        self.repo = elapsed_items_repo

    def handle_batch(self, batch):
        print(batch[0])
        self.repo.upsert_batch([ElapsedTimePayload(**elapsed_items_tuple[1]) for elapsed_items_tuple in batch])


elapsed_items_repo = ElapsedTimeRepository(driver)

elapsed_items_consumer = ElapsedItemConsumer(elapsed_items_repo, redis, "elapsed_items", "group", "elapsed_items", 1000, 1)

elapsed_items_consumer.run()
