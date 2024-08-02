"""
ETL class for data transfer
"""

from io import StringIO
import s3fs
import pandas as pd
from pyarrow import Table, parquet as pq
import psycopg2


class Pipeline:
    """_summary_"""

    def __init__(self) -> None:
        pass

    def put_to_fs(self, df: pd.DataFrame, path: str, fs: s3fs.S3FileSystem):
        """
        Write a DataFrame to a Parquet file on S3
        Args:
            df (pd.DataFrame): DataFrame to write
            path (str): S3 path to write the DataFrame to
        """
        table = Table.from_pandas(df)
        pq.write_to_dataset(
            table=table,
            root_path=path,
            filesystem=fs,
            use_dictionary=True,
            compression="snappy",
            version="2.0",
        )

    def read_from_fs(self, path: str, fs: s3fs.S3FileSystem) -> pd.DataFrame:
        """
        Read a Parquet file from S3 into a DataFrame
        Args:
            path (str): S3 path to read the DataFrame from
        Returns:
            pd.DataFrame: DataFrame read from the Parquet file
        """
        for f in fs.glob(f"{path}*"):
            table = pq.read_table(f, filesystem=fs)
        return table.to_pandas()

    def local_to_fs_transfer(
        self, local_path: str, fs_path: str, fs: s3fs.S3FileSystem
    ):
        """
        Transfer data from local to FS
        Args:
            local_path (str): local path to read the DataFrame from
            fs_path (str): fs path to write the DataFrame to
            fs (s3fs.S3FileSystem): file system client
        """
        df = pd.read_parquet(local_path)
        self.put_to_fs(df=df, path=fs_path, fs=fs)

    def write_to_postgres(self, df: pd.DataFrame, config: dict, table_name: str):
        """
        Write a DataFrame to a Postgres table
        Args:
            df (pd.DataFrame): data to write
            config (dict): postgres connection configuration
            table_name (str): table name to write to
        """
        conn = psycopg2.connect(**config)
        sio = StringIO()
        df.to_csv(sio, index=None, header=None)
        sio.seek(0)
        with conn.cursor() as c:
            c.copy_expert(
                sql=f"""
                COPY {table_name} (
                    {",".join(df.columns)}
                ) FROM STDIN WITH CSV""",
                file=sio,
            )
        conn.commit()

    def fs_to_postgres_transfer(
        self, path: str, fs: s3fs.S3FileSystem, config: dict, table_name: str
    ):
        """
        transfer data from FS to Postgres
        Args:
            path (str): path to read the DataFrame from
            fs (s3fs.S3FileSystem): file system client
            config (dict): postgres connection configuration
            table_name (str): table name to write to
        """
        df = self.read_from_fs(path=path, fs=fs)
        self.write_to_postgres(df=df, config=config, table_name=table_name)

    def local_to_postgres_transfer(
        self, local_path: str, config: dict, table_name: str
    ):
        """
        Transfer data from local to Postgres
        Args:
            local_path (str): local path to read the DataFrame from
            config (dict): postgres connection configuration
            table_name (str): table name to write to
        """
        df = pd.read_parquet(local_path)
        self.write_to_postgres(df=df, config=config, table_name=table_name)
