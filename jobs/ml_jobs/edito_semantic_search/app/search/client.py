	

import lancedb
from app.constants import embedding_model
from loguru import logger
import polars as pl
class SearchClient:
	def __init__(self, database_uri: str, vector_table: str, scalar_table: str):
		"""Connects to LanceDB and opens the specified table."""
		self.embedding_model = embedding_model
		logger.info(f"Connecting to LanceDB at: {database_uri}")
		self.db = lancedb.connect(database_uri)
		logger.info(f"Opening vector table: {vector_table} and scalar table: {scalar_table}")
		self.vector_table = self.db.open_table(vector_table)
		logger.info(f"Vector table successfully opened.")
		# self.scalar_table = self.db.open_table(scalar_table)

    
	def build_where_clause(self, filters):
		"""
		Build a SQL WHERE clause from a list of filter dicts.
		Each filter dict should have: column, operator, value.
		"""
		clauses = []
		for f in filters:
			col = f["column"]
			op = f["operator"]
			val = f["value"]
			if op.lower() == "in" and isinstance(val, (list, tuple, set)):
				val_str = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in val])
				clause = f"{col} IN ({val_str})"
			elif isinstance(val, str):
				clause = f"{col} {op} '{val}'"
			else:
				clause = f"{col} {op} {val}"
			clauses.append(f"({clause})")
		return " AND ".join(clauses)

	def scalar_search(self, k: int = 1000, filters: list[dict] = None):
		"""Performs a scalar search using polars lazy mode on a Parquet directory in GCS/local (supports data-*.parquet)."""
		import polars as pl
		try:
			from app.constants import PARQUET_FILE
			parquet_dir = PARQUET_FILE  # should be the directory path
			logger.info(f"Reading Parquet directory (lazy) from: {parquet_dir}")
			lf = pl.scan_parquet(f"{parquet_dir}/data-*.parquet")
			if filters:
				logger.info(f"Applying filters: {filters}")
				for f in filters:
					col = f["column"]
					op = f["operator"].lower()
					val = f["value"]
					if op == "in" and isinstance(val, (list, tuple, set)):
						lf = lf.filter(pl.col(col).is_in(val))
					elif op == "=":
						lf = lf.filter(pl.col(col) == val)
					elif op == ">":
						lf = lf.filter(pl.col(col) > val)
					elif op == ">=":
						lf = lf.filter(pl.col(col) >= val)
					elif op == "<":
						lf = lf.filter(pl.col(col) < val)
					elif op == "<=":
						lf = lf.filter(pl.col(col) <= val)
					elif op == "!=" or op == "<>":
						lf = lf.filter(pl.col(col) != val)
					else:
						logger.warning(f"Unsupported operator: {op}")
			df = lf.head(k).collect()
			result = df.to_dicts()
			return result
		except Exception as e:
			logger.error(f"Scalar search error: {e}")
			return None

	def vector_search(self, query_vector, k: int = 5, filters: list[dict] = None):
		"""Performs a vector similarity search using LanceDB's search method."""
		try:
			if filters:
				where_clause = self.build_where_clause(filters)
				result = self.vector_table.search(query_vector).where(where_clause,prefilter=True).limit(k).to_list()
			else:
				result = self.vector_table.search(query_vector).limit(k).to_list()
			return result
		except Exception as e:
			print(f"Vector search error: {e}")
			return None
    