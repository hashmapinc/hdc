import os

from hdc.core.catalog.crawler.netezza_crawler import NetezzaCrawler
from hdc.core.catalog.ddl_writer.snowflake_ddl_writer import SnowflakeDDLWriter
from hdc.core.catalog.mapper.netezza_to_snowflake_mapper import NetezzaToSnowflakeMapper


class PackageFactory:
    @classmethod
    def catalog(cls, source_env: str, path: str) -> tuple:
        os.environ['HDC_PROFILE_PATH'] = path
        crawler = NetezzaCrawler(connection_name=source_env)
        return crawler.run()

    @classmethod
    def map(cls, data_tuple) -> tuple:
        mapper = NetezzaToSnowflakeMapper(databases=data_tuple[0], schemas=data_tuple[1], tables=data_tuple[2])
        return mapper.map()

    @classmethod
    def write(cls, destination_env: str, path: str, sql_tuple) -> None:
        os.environ['HDC_PROFILE_PATH'] = path
        writer = SnowflakeDDLWriter(connection_name=destination_env, database_sql=sql_tuple[0], schema_sql=sql_tuple[1], table_sql=sql_tuple[2])
        writer.execute()
