	

import lancedb
from app.constants import embedding_model
from loguru import logger
class SearchClient:
	def __init__(self, database_uri: str, vector_table: str, scalar_table: str):
		"""Connects to LanceDB and opens the specified table."""
		self.embedding_model = embedding_model
		self.db = lancedb.connect(database_uri)
		self.vector_table = self.db.open_table(vector_table)
		self.scalar_table = self.db.open_table(scalar_table)

    
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

	def scalar_search(self,k:int=1000, filters:list[dict]=None):
		"""Performs a scalar search on the given column for the specified value."""
		try:
			# Use pandas query for filtering
			where_clause = self.build_where_clause(filters)
			logger.info(f"Scalar search where clause: {where_clause}")
			result = self.scalar_table.search().where(f"{where_clause}").limit(k).to_list()
			return result
		except Exception as e:
			print(f"Scalar search error: {e}")
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
    