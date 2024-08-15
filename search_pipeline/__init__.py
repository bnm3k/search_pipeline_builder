from .base_searchers import *
from .fusion_method import *
from .rerankers import *

from typing import Callable, Optional


def create_search_fn(
    base_searchers: BaseSearcher | list[BaseSearcher],
    *,
    fusion_method: Optional[FusionMethod] = None,
    reranker: Optional[Reranker] = None,
) -> Callable[[str], SearchResult]:
    if isinstance(base_searchers, BaseSearcher):
        base_searchers = [base_searchers]

    # add base searcher(s) and fusion method
    if len(base_searchers) == 0:
        raise Exception("Base_searchers should be > 0")
    elif len(base_searchers) == 1:
        assert (
            fusion_method is None
        ), "You provided a fusion method yet there is only 1 base searcher"
        first = base_searchers[0]
        searcher = lambda q: first.search(q)
    else:  # >= 2
        assert (
            fusion_method is not None
        ), "You must provide a fusion method if you provide >= 2 base searchers"
        searcher = lambda q: fusion_method.fuse(
            [s.search(q) for s in base_searchers]
        )

    # add reranker
    if reranker is not None:
        searcher_with_reranking = lambda q: reranker.rerank(q, searcher(q))
        return searcher_with_reranking
    else:
        return searcher
