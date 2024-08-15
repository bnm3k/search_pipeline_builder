from abc import ABC, abstractmethod

import duckdb
import pyarrow as pa

from .common import SearchResult, RankMetric


class Reranker(ABC):
    conn: duckdb.DuckDBPyConnection

    @abstractmethod
    def rerank(self, query: str, search_result: SearchResult) -> SearchResult:
        ...

    def retrieve_docs(self, results: pa.Table) -> pa.Table:
        return self.conn.sql(
            """
        select id, d.doc
        from results r
        join documents d using(id)
        """
        ).arrow()


class MSMarcoCrossEncoder(Reranker):
    def __init__(self, conn):
        from sentence_transformers import CrossEncoder

        self.conn = conn
        self.encoder = CrossEncoder(
            "cross-encoder/ms-marco-MiniLM-L-6-v2",
        )

    def rerank(self, query: str, search_result: SearchResult) -> SearchResult:
        docs = self.retrieve_docs(search_result.tbl)

        # rerank
        scores = self.encoder.predict(
            [(query, v) for v in docs["doc"].to_pylist()],
        )
        reranked_results = pa.Table.from_arrays(
            [docs["id"], scores], names=["id", "score"]
        )
        return SearchResult(reranked_results, RankMetric.SCORE, sorted=False)


class ColbertReranker(Reranker):
    def __init__(self, conn, max_count: int):
        from ragatouille import RAGPretrainedModel

        self.conn = conn
        self.colbert = RAGPretrainedModel.from_pretrained(
            "colbert-ir/colbertv2.0"
        )
        self.max_count = max_count

    def rerank(self, query: str, search_result: SearchResult) -> SearchResult:
        docs = self.retrieve_docs(search_result.tbl)
        reranked_docs = self.colbert.rerank(
            query=query,
            documents=[d for d in docs["doc"].to_pylist()],
            k=self.max_count,
        )
        doc_ids = docs["id"].to_pylist()
        reranked_results = pa.Table.from_arrays(
            [
                (doc_ids[r["result_index"]] for r in reranked_docs),
                (r["score"] for r in reranked_docs),
            ],
            names=["id", "score"],
        )

        return SearchResult(reranked_results, RankMetric.SCORE, sorted=True)


class JinaRerankerV2(Reranker):
    """Note, is licensed under CC"""

    def __init__(self, conn):
        from transformers import AutoModelForSequenceClassification

        self.conn = conn

        model = AutoModelForSequenceClassification.from_pretrained(
            "jinaai/jina-reranker-v2-base-multilingual",
            torch_dtype="auto",
            trust_remote_code=True,
            local_files_only=True,
        )

        model.to("cuda")  # or 'cpu' if no GPU is available
        model.eval()
        self.model = model

    def rerank(self, query: str, search_result: SearchResult) -> SearchResult:
        docs = self.retrieve_docs(search_result.tbl)

        # rerank
        sentence_pairs = [[query, doc] for doc in docs["doc"].to_pylist()]
        scores = self.model.compute_score(sentence_pairs, max_length=1024)
        reranked_results = pa.Table.from_arrays(
            [docs["id"], scores], names=["id", "score"]
        )
        return SearchResult(reranked_results, RankMetric.SCORE, sorted=False)
