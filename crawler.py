import time, random, requests
from typing import Dict, Any, List
from db import upsert_rows

BASE_URL = "https://api.map.baidu.com/place/v2/search"
PAGE_SIZE = 20
MAX_RETRY = 3
RETRY_BACKOFF = 1.6

def build_params(ak: str, region: str, query: str, page_num: int,
                 ret_coordtype: str = "gcj02ll", city_limit=True):
    p = {
        "query": query,
        "region": region,
        "city_limit": "true" if city_limit else "false",
        "output": "json",
        "ak": ak,
        "page_size": PAGE_SIZE,
        "page_num": page_num,
        "scope": 2,
        "extensions_adcode": "true",
        "ret_coordtype": ret_coordtype
    }
    return p

def normalize_rows(results: List[Dict[str, Any]], query: str):
    rows = []
    for r in results:
        loc = r.get("location") or {}
        det = r.get("detail_info") or {}
        rows.append({
            "uid": r.get("uid",""),
            "name": r.get("name",""),
            "address": r.get("address",""),
            "province": r.get("province",""),
            "city": r.get("city",""),
            "area": r.get("area",""),
            "adcode": r.get("adcode",""),
            "lat": loc.get("lat"),
            "lng": loc.get("lng"),
            "type": r.get("type",""),
            "tag": r.get("tag",""),
            "classified_poi_tag": r.get("classified_poi_tag",""),
            "telephone": r.get("telephone",""),
            "detail": r.get("detail",0),
            "overall_rating": det.get("overall_rating",""),
            "price": det.get("price",""),
            "shop_hours": det.get("shop_hours",""),
            "brand": det.get("brand",""),
            "content_tag": det.get("content_tag",""),
            "source_query": query
        })
    return rows

def request_once(params: Dict[str, Any]):
    last_err = None
    for i in range(1, MAX_RETRY+1):
        try:
            r = requests.get(BASE_URL, params=params, timeout=18)
            if r.status_code == 200:
                data = r.json()
                if data.get("status") == 0:
                    return data
                last_err = RuntimeError(f"API status={data.get('status')} msg={data.get('message')}")
            else:
                last_err = RuntimeError(f"HTTP {r.status_code}")
        except Exception as e:
            last_err = e
        time.sleep((0.35 + random.random()*0.25) * (RETRY_BACKOFF**(i-1)))
    raise last_err or RuntimeError("unknown error")

def crawl_region(ak: str, region: str, queries: List[str], qps: float = 2.0, city_limit=True):
    sleep_base = 1.0 / max(0.5, qps)
    total = 0
    per_query_stats = []
    for q in queries:
        page = 0
        got = 0
        while True:
            data = request_once(build_params(ak, region, q, page, city_limit=city_limit))
            res = data.get("results") or []
            if not res:
                break
            upsert_rows(normalize_rows(res, q))
            got += len(res)
            page += 1
            time.sleep(sleep_base)
        per_query_stats.append({"query": q, "count": got})
        total += got
    return {"inserted_or_updated": total, "per_query": per_query_stats}
