import asyncio
import os
import logging
from typing import List, Dict, Any, Optional
import motor.motor_asyncio
from pymongo.errors import ConnectionFailure, BulkWriteError
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
import tqdm
import tqdm.asyncio

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

LOG_FILE_PATH = os.path.join(ROOT_DIR, "production.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE_PATH), logging.StreamHandler()],
)

log = logging.getLogger(__name__)


class MongoManager:
    """
    An asynchronous manager for handling high-performance MongoDB connections
    and operations using 'motor'.
    """

    def __init__(self, uri: str, db_name: str):
        """
        Initializes the manager with the connection URI and database name.

        Args:
            uri (str): The MongoDB connection string (e.g., "mongodb://...").
            db_name (str): The name of the database to connect to.
        """
        if not uri or not db_name:
            raise ValueError("MongoDB URI and database name must be provided.")
        self.client: Optional[motor.motor_asyncio.AsyncIOMotorClient] = None
        self.db: Optional[motor.motor_asyncio.AsyncIOMotorDatabase] = None
        self.uri = uri
        self.db_name = db_name
        log.info(f"MongoManager initialized for database: {db_name}")

    async def connect(self):
        """
        Establishes the asynchronous connection to the MongoDB server.
        """
        if self.client:
            log.info("Already connected.")
            return

        log.info(f"Connecting to MongoDB at {self.uri.split('@')[-1]}...")
        try:
            self.client = motor.motor_asyncio.AsyncIOMotorClient(self.uri)
            await self.client.admin.command("ping")
            self.db = self.client[self.db_name]
            log.info(f"Successfully connected to MongoDB. Database: '{self.db_name}'")
        except ConnectionFailure as e:
            log.error(f"Failed to connect to MongoDB: {e}")
            self.client = None
            self.db = None
            raise

    async def close(self):
        """
        Closes the connection to the MongoDB server.
        """
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
            log.info("MongoDB connection closed.")

    async def push_data_parallel(
        self,
        collection_name: str,
        data: List[Dict[str, Any]],
        chunk_size: int = 1000,
    ) -> int:
        """
        Pushes a large list of documents to a collection in parallel.

        Splits the data into chunks and uses asyncio.gather to execute
        insert_many operations concurrently. Uses 'ordered=False' for
        maximum server-side insert performance.

        Args:
            collection_name (str): The name of the collection.
            data (List[Dict[str, Any]]): A list of documents to insert.
            chunk_size (int, optional): The number of documents per parallel chunk.
                                        Defaults to 1000.

        Returns:
            int: The total number of documents successfully inserted.
        """
        if self.db is None:
            raise RuntimeError("Not connected. Call connect() first.")
        if not data:
            log.warning("No data provided to push.")
            return 0

        collection = self.db[collection_name]
        tasks = []

        for i in range(0, len(data), chunk_size):
            chunk = data[i : i + chunk_size]
            tasks.append(collection.insert_many(chunk, ordered=False))

        log.info(f"Pushing {len(data)} documents in {len(tasks)} parallel chunks...")

        results = await asyncio.gather(*tasks, return_exceptions=True)

        total_inserted = 0
        for res in results:
            if isinstance(res, BulkWriteError):
                log.error(f"A chunk failed to insert: {res.details}")
                total_inserted += res.details.get("nInserted", 0)
            elif isinstance(res, Exception):
                log.error(f"An unexpected error occurred during insertion: {res}")
            else:
                total_inserted += len(res.inserted_ids)

        log.info(f"Successfully inserted {total_inserted} / {len(data)} documents.")
        return total_inserted

    def pull_data_cursor(
        self,
        collection_name: str,
        query: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, Any]] = None,
    ) -> motor.motor_asyncio.AsyncIOMotorCursor:
        """
        Gets an asynchronous cursor to efficiently stream data from a collection.

        This is the most memory-efficient way to handle large datasets,
        as it fetches documents in batches rather than all at once.

        Args:
            collection_name (str): The name of the collection.
            query (Optional[Dict[str, Any]], optional): A MongoDB query filter.
                                                        Defaults to None (match all).
            projection (Optional[Dict[str, Any]], optional): A MongoDB projection
                                                            to select fields (e.g., {"name": 1, "_id": 0}).
                                                            Defaults to None (return all fields).

        Returns:
            motor.motor_asyncio.AsyncIOMotorCursor: An async cursor.
        """
        if self.db is None:
            raise RuntimeError("Not connected. Call connect() first.")

        log.info(f"Creating data cursor for collection '{collection_name}'...")
        collection = self.db[collection_name]
        return collection.find(query or {}, projection)

    async def pull_data_to_file_in_chunks(
        self,
        collection_name: str,
        output_dir: str,
        output_filename: str,
        query: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, Any]] = None,
        chunk_size: int = 1_000_000,
    ) -> str:
        """
        Pulls a large collection to a single Parquet file by chunking.

        1. Counts total documents.
        2. Streams data, saving to temporary chunk files (Parquet format).
        3. Concatenates all chunks into one final file (memory-efficiently).
        4. Cleans up temporary files.

        Args:
            collection_name (str): The name of the collection.
            output_dir (str): Directory to save the final file.
            output_filename (str): Name of the final output file (e.g., "export.parquet").
            query (Optional[Dict[str, Any]], optional): MongoDB query filter.
            projection (Optional[Dict[str, Any]], optional): MongoDB projection.
            chunk_size (int, optional): Docs per chunk. Defaults to 1,000,000.

        Returns:
            str: The full path to the final concatenated file.
        """
        if self.db is None:
            raise RuntimeError("Not connected. Call connect() first.")

        collection = self.db[collection_name]
        query = query or {}

        log.info(f"Counting documents for collection '{collection_name}'...")
        total_docs = await collection.count_documents(query)
        log.info(f"Total documents to pull: {total_docs}")

        if total_docs == 0:
            log.warning("No documents found. Nothing to pull.")
            return ""

        os.makedirs(output_dir, exist_ok=True)
        temp_chunk_dir = os.path.join(output_dir, "temp_mongo_chunks_parquet")
        os.makedirs(temp_chunk_dir, exist_ok=True)
        final_file_path = os.path.join(output_dir, output_filename)
        chunk_files_list = []

        log.info(f"Pulling {total_docs} documents in chunks of {chunk_size}...")

        chunk_num = 0
        current_chunk_data = []

        try:
            cursor = self.pull_data_cursor(collection_name, query, projection)

            async for document in tqdm.asyncio.tqdm(
                cursor, total=total_docs, desc="Pulling documents", unit="doc"
            ):
                current_chunk_data.append(document)

                if len(current_chunk_data) >= chunk_size:
                    chunk_file_path = os.path.join(
                        temp_chunk_dir, f"chunk_{chunk_num}.parquet"
                    )
                    log.debug(f"Writing chunk {chunk_num} to {chunk_file_path}")

                    df = pd.DataFrame(current_chunk_data)
                    table = pa.Table.from_pandas(df, preserve_index=False)
                    pq.write_table(table, chunk_file_path)

                    chunk_files_list.append(chunk_file_path)
                    current_chunk_data = []
                    chunk_num += 1

            if current_chunk_data:
                chunk_file_path = os.path.join(
                    temp_chunk_dir, f"chunk_{chunk_num}.parquet"
                )
                log.debug(f"Writing final chunk {chunk_num} to {chunk_file_path}")

                df = pd.DataFrame(current_chunk_data)
                table = pa.Table.from_pandas(df, preserve_index=False)
                pq.write_table(table, chunk_file_path)

                chunk_files_list.append(chunk_file_path)
                current_chunk_data = []  # Clear memory

        finally:
            log.info(
                f"Finished pulling all data into {len(chunk_files_list)} chunk files."
            )

        if not chunk_files_list:
            log.warning("No chunk files were created. Aborting.")
            return ""

        log.info(
            f"Concatenating {len(chunk_files_list)} chunks into {final_file_path}..."
        )
        try:
            schema = pq.read_schema(chunk_files_list[0])

            with pq.ParquetWriter(final_file_path, schema=schema) as writer:
                for chunk_file_path in tqdm.tqdm(
                    chunk_files_list, desc="Concatenating chunks", unit="chunk"
                ):
                    table = pq.read_table(chunk_file_path)

                    writer.write_table(table)

        except Exception as e:
            log.error(f"Failed during Parquet concatenation: {e}")
            raise

        log.info(f"Cleaning up temporary chunks from {temp_chunk_dir}...")
        try:
            shutil.rmtree(temp_chunk_dir)
        except Exception as e:
            log.error(f"Could not remove temp directory {temp_chunk_dir}: {e}")

        log.info(f"Data pull complete. Final file saved to: {final_file_path}")
        return final_file_path
