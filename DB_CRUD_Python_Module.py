"""
DB_CRUD_Python_Module.py

Provides a reusable CRUD abstraction layer for interacting with
MongoDB animal shelter data.

Features:
- Environment-safe connection
- Indexed collections
- Projection support
- Pagination support
- Aggregation pipelines
"""

from pymongo import MongoClient, errors, ASCENDING, DESCENDING
from bson.objectid import ObjectId


class Animal_Shelter(object):
    """
    Encapsulates CRUD operations for the animals collection.
    """

    def __init__(self, user, password, host, port, database, collection, auth_source=None):
        """
        Establish MongoDB connection and ensure indexes exist.
        """

        USER = user
        PASS = password
        HOST = host
        PORT = int(port)
        DB = database
        COL = collection
        AUTH = auth_source or DB

        self.client = MongoClient(
            f"mongodb://{USER}:{PASS}@{HOST}:{PORT}/{DB}?authSource={AUTH}"
        )

        self.database = self.client[DB]
        self.collection = self.database[COL]

        # Ensure indexes on startup
        self.ensure_indexes()


    def ensure_indexes(self):
        """
        Creates indexes aligned with common dashboard queries.
        """

        try:
            self.collection.create_index(
                [
                    ("animal_type", ASCENDING),
                    ("sex_upon_outcome", ASCENDING),
                    ("age_upon_outcome_in_weeks", ASCENDING),
                    ("breed", ASCENDING),
                ],
                name="idx_rescue_filters"
            )

            self.collection.create_index(
                [("location_lat", ASCENDING), ("location_long", ASCENDING)],
                name="idx_location"
            )

        except errors.PyMongoError as e:
            print(f"[WARN] Could not ensure indexes: {e}")


    def getNextRecordNum(self):
        """
        Returns next available rec_num value.
        """

        out = self.database.animals.find().sort([('rec_num', -1)]).limit(1)

        for doc in out:
            return doc['rec_num'] + 1


    # ============================
    # CREATE
    # ============================

    def create(self, data):
        """
        Inserts a new document.
        """

        if data is None:
            raise Exception("Nothing to save, data parameter is empty")

        try:
            self.database.animals.insert_one(data)
            return True
        except errors.PyMongoError as e:
            print(f"Insert failed: {e}")
            return False


    # ============================
    # READ
    # ============================

    def read(self, query, projection=None, limit=0, skip=0, sort=None):
        """
        Reads documents using optional projection, pagination, and sorting.
        """

        if query is None:
            raise Exception("Query parameter is empty")

        try:
            cursor = self.collection.find(query, projection)

            if sort:
                normalized = [
                    (field, ASCENDING if direction >= 0 else DESCENDING)
                    for field, direction in sort
                ]
                cursor = cursor.sort(normalized)

            if skip > 0:
                cursor = cursor.skip(int(skip))

            if limit > 0:
                cursor = cursor.limit(int(limit))

            return list(cursor)

        except errors.PyMongoError as e:
            print(f"Read failed: {e}")
            return []


    # ============================
    # UPDATE
    # ============================

    def update(self, lookup_pair, update_data):
        """
        Updates matching documents.
        """

        if lookup_pair is None or update_data is None:
            raise Exception("lookup_pair and update_data are required")

        try:
            if not any(k.startswith('$') for k in update_data):
                update_data = {'$set': update_data}

            result = self.collection.update_many(lookup_pair, update_data)
            return result.modified_count

        except errors.PyMongoError as e:
            print(f"Update failed: {e}")
            return 0


    # ============================
    # DELETE
    # ============================

    def delete(self, lookup_pair):
        """
        Deletes matching documents.
        """

        if lookup_pair is None:
            raise Exception("lookup_pair is required")

        try:
            result = self.collection.delete_many(lookup_pair)
            return result.deleted_count
        except errors.PyMongoError as e:
            print(f"Delete failed: {e}")
            return 0


    # ============================
    # AGGREGATION
    # ============================

    def breed_counts(self, query, limit=20):
        """
        Returns top breeds and counts using aggregation pipeline.
        """

        query = query or {}

        pipeline = [
            {"$match": query},
            {"$group": {"_id": "$breed", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": int(limit)}
        ]

        try:
            results = list(self.collection.aggregate(pipeline))
            return [{"breed": r["_id"], "count": r["count"]} for r in results if r["_id"]]
        except errors.PyMongoError as e:
            print(f"Aggregation failed: {e}")
            return []


    # ============================
    # COUNT
    # ============================

    def count(self, query=None) -> int:
        """
        Returns count of matching documents.
        Used for pagination.
        """

        query = query or {}

        try:
            return int(self.collection.count_documents(query))
        except errors.PyMongoError as e:
            print(f"Count failed: {e}")
            return 0
