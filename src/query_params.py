"""Param factory / registry for different query types."""

from typing import Dict, Callable, Any

def _metric_params(job: Dict[str, Any]) -> Dict[str, Any]:
    inputs = job.get("inputs", {})
    required = ["metric", "client", "date_start", "date_end"]
    missing = [r for r in required if r not in inputs]
    if missing:
        raise KeyError(f"Missing required inputs for metric query: {missing}")
    return {
        "metric": inputs["metric"].lower(),
        "icp_client": inputs["client"],
        "start_date": inputs["date_start"],
        "end_date": inputs["date_end"],
    }


def _smart_offer_params(job: Dict[str, Any]) -> Dict[str, Any]:
    inputs = job.get("inputs", {})
    required = ["client", "business_unit", "date_start", "date_end"]
    missing = [r for r in required if r not in inputs]
    if missing:
        raise KeyError(f"Missing required inputs for smart_offer query: {missing}")
    return {
        "client": inputs["client"],
        "business_unit": inputs["business_unit"],
        "start_date": inputs["date_start"],
        "end_date": inputs["date_end"],
    }


# Registry: add new query types here
QUERY_PARAM_BUILDERS: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = {
    "metric": _metric_params,
    "smart_offer": _smart_offer_params,
}


def build_query_params(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build SQL params for the job using registered param builders.
    Applies job["query"]["extra_params"] as overrides.
    """
    q = job.get("query", {})
    qtype = q.get("query_type")
    if not qtype:
        raise KeyError("Job['query']['query_type'] is required")

    builder = QUERY_PARAM_BUILDERS.get(qtype)
    if builder is None:
        raise ValueError(f"Unsupported query_type: {qtype}. Available: {list(QUERY_PARAM_BUILDERS.keys())}")

    params = builder(job)
    extra = q.get("extra_params", {}) or {}
    # override / extend
    params.update(extra)
    return params
