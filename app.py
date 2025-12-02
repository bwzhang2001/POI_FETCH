import io
import csv
import json
import time
from pathlib import Path
from typing import List, Dict, Any
from collections import OrderedDict
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from flask import Flask, render_template, request, jsonify, Response, send_file

from crawler import crawl_region
from db import fetch_geojson, list_categories

app = Flask(__name__, template_folder="templates", static_folder="static")

DEFAULT_QUERIES = [
    "美食","酒店","购物","生活服务","休闲娱乐","运动健身","教育培训",
    "医疗","汽车服务","交通设施","金融","房地产","公司企业","政府机构",
    "旅游景点","自然地物","公共设施","商务住宅","物流仓储","房产小区",
    "加油站","停车场","银行","超市","便利店","景点","博物馆","图书馆",
    "体育场馆","电影院","咖啡厅","茶馆","酒吧"
]

REG_FILE = Path("regions.json")
CSV_FIELDS = [
    "uid","name","address","province","city","area","adcode","lat","lng","type","tag",
    "classified_poi_tag","telephone","detail","overall_rating","price","shop_hours","brand",
    "content_tag","source_query"
]

FALLBACK_REGIONS = {
    "甘肃省": {
        "兰州市": ["城关区","七里河区","西固区","安宁区","红古区","永登县","皋兰县","榆中县"]
    }
}

BAIDU_REGION_API = "https://api.map.baidu.com/api_region_search/v1/"
SKIP_CITY_NAMES = {"市辖区"}
MUNICIPALITIES = {"北京","北京市","天津","天津市","上海","上海市","重庆","重庆市"}

UPLOAD_DIR = Path("./_uploads"); UPLOAD_DIR.mkdir(exist_ok=True)
EXPORT_DIR = Path("./_exports"); EXPORT_DIR.mkdir(exist_ok=True)

LATEST_GEOJSON: Dict[str, Any] = {}

def _http_get(params: Dict[str, Any], retry: int = 3, backoff: float = 1.5) -> Dict[str, Any]:
    last = None
    for i in range(retry):
        try:
            r = requests.get(BAIDU_REGION_API, params=params, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep((backoff ** i) * 0.7)
    raise last or RuntimeError("request failed")

def _get_name(node: Dict[str, Any]) -> str:
    for k in ("name","fullname","city","province","district","text","area_name","title"):
        v = node.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""

def _children(node: Dict[str, Any]):
    for k in ("sub","children","districts","sub_admin","areas","list","items"):
        v = node.get(k)
        if isinstance(v, list):
            return v
    return []

def _walk_lists(obj):
    if isinstance(obj, list):
        yield obj
        for it in obj:
            yield from _walk_lists(it)
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from _walk_lists(v)

def _is_municipality(pname: str) -> bool:
    return pname in MUNICIPALITIES

def _looks_like_province_list(lst):
    if not (isinstance(lst, list) and lst and all(isinstance(x, dict) for x in lst)):
        return False
    got_name = sum(1 for x in lst if _get_name(x))
    return got_name >= max(5, len(lst)//3)

def _extract_provinces_from_resp(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ("data","result","districts","records","list"):
        val = resp.get(key)
        if isinstance(val, list) and _looks_like_province_list(val):
            return val
        if isinstance(val, dict):
            for k2 in ("data","result","districts","list","province","provinces"):
                v2 = val.get(k2)
                if isinstance(v2, list) and _looks_like_province_list(v2):
                    return v2
    for lst in _walk_lists(resp):
        if _looks_like_province_list(lst):
            if len(lst) == 1 and _children(lst[0]):
                return _children(lst[0])
            return lst
    return []

def _normalize_mapping(provinces: List[Dict[str, Any]],
                       exclude_hkm: bool = False,
                       exclude_tw: bool = False) -> Dict[str, Dict[str, List[str]]]:
    mapping = OrderedDict()
    for p in provinces:
        prov = _get_name(p)
        if not prov:
            continue
        if exclude_hkm and prov in {"香港特别行政区","澳门特别行政区","香港","澳门"}:
            continue
        if exclude_tw and prov in {"台湾省","台湾"}:
            continue

        if _is_municipality(prov):
            city_name = prov if prov.endswith("市") else prov + "市"
            dists = []
            for c in _children(p):
                cname = _get_name(c)
                if cname in SKIP_CITY_NAMES:
                    for d in _children(c):
                        nm = _get_name(d)
                        if nm and nm not in SKIP_CITY_NAMES:
                            dists.append(nm)
                else:
                    subs = _children(c)
                    if subs:
                        for d in subs:
                            nm = _get_name(d)
                            if nm and nm not in SKIP_CITY_NAMES:
                                dists.append(nm)
            mapping[prov if prov.endswith("市") else prov] = OrderedDict({city_name: list(dict.fromkeys(dists))})
            continue

        city_map = OrderedDict()
        for c in _children(p):
            cname = _get_name(c)
            if not cname:
                continue

            if cname in SKIP_CITY_NAMES:
                holder = "省直辖县级行政区"
                arr = city_map.setdefault(holder, [])
                for d in _children(c):
                    nm = _get_name(d)
                    if nm and nm not in SKIP_CITY_NAMES:
                        arr.append(nm)
                city_map[holder] = list(dict.fromkeys(arr))
                continue

            subs = _children(c)
            if subs:
                dlist = []
                for d in subs:
                    nm = _get_name(d)
                    if nm and nm not in SKIP_CITY_NAMES:
                        dlist.append(nm)
                city_map[cname] = list(dict.fromkeys(dlist))
            else:
                holder = "省直辖县级行政区"
                arr = city_map.setdefault(holder, [])
                if cname not in arr:
                    arr.append(cname)
                city_map[holder] = list(dict.fromkeys(arr))

        mapping[prov] = city_map

    return mapping

def _fetch_all_once(ak: str) -> List[Dict[str, Any]]:
    d = _http_get({"keyword":"中国","sub_admin":3,"extensions_code":1,"ak":ak})
    if d.get("status") not in (0, "0"):
        raise RuntimeError(f"API status={d.get('status')} msg={d.get('message') or d.get('msg')}")
    provs = _extract_provinces_from_resp(d)
    if not provs:
        raise RuntimeError("返回为空（未找到省级列表）")
    return provs

def _fetch_all_by_province(ak: str) -> List[Dict[str, Any]]:
    d = _http_get({"keyword":"中国","sub_admin":1,"extensions_code":1,"ak":ak})
    if d.get("status") not in (0, "0"):
        raise RuntimeError(f"API status={d.get('status')} msg={d.get('message') or d.get('msg')}")
    root = _extract_provinces_from_resp(d)
    if not root:
        raise RuntimeError("返回为空（省级列表）")
    provinces = []
    for p in root:
        pname = _get_name(p)
        if not pname:
            continue
        d2 = _http_get({"keyword": pname, "sub_admin": 2, "extensions_code": 1, "ak": ak})
        if d2.get("status") not in (0, "0"):
            raise RuntimeError(f"[{pname}] API status={d2.get('status')} msg={d2.get('message') or d2.get('msg')}")
        node_list = _extract_provinces_from_resp(d2)
        if node_list and _get_name(node_list[0]) == pname:
            node = node_list[0]
        else:
            node = {"name": pname, "sub": node_list}
        provinces.append(node)
        time.sleep(0.25)
    return provinces

def _ensure_regions_cache(ak: str, refresh: bool = False,
                          exclude_hkm: bool = False, exclude_tw: bool = False) -> Dict[str, Any]:
    if REG_FILE.exists() and not refresh:
        try:
            data = json.loads(REG_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict) and data:
                return data
        except Exception:
            pass

    if not ak:
        REG_FILE.write_text(json.dumps(FALLBACK_REGIONS, ensure_ascii=False, indent=2), encoding="utf-8")
        return FALLBACK_REGIONS

    try:
        provs = _fetch_all_once(ak)
    except Exception:
        provs = _fetch_all_by_province(ak)

    mapping = _normalize_mapping(provs, exclude_hkm=exclude_hkm, exclude_tw=exclude_tw)
    if not mapping:
        mapping = FALLBACK_REGIONS
    REG_FILE.write_text(json.dumps(mapping, ensure_ascii=False, indent=2), encoding="utf-8")
    return mapping

@app.route("/")
def index():
    return render_template("index.html", default_queries=",".join(DEFAULT_QUERIES))

@app.route("/regions")
def regions():
    ak = (request.args.get("ak") or "").strip()
    refresh = request.args.get("refresh", "0").lower() in ("1", "true", "yes")
    try:
        mapping = _ensure_regions_cache(ak=ak, refresh=refresh)
        return jsonify(mapping)
    except Exception as e:
        return jsonify({"__error": str(e)}), 200

@app.route("/crawl", methods=["POST"])
def crawl():
    try:
        data = request.get_json(force=True)
    except Exception:
        return jsonify({"ok": False, "error": "请求体不是合法 JSON"}), 400

    ak = (data.get("ak") or "").strip()
    if not ak:
        return jsonify({"ok": False, "error": "缺少 AK"}), 400

    reg_json = _ensure_regions_cache(ak=ak, refresh=False)

    province = (data.get("province") or "").strip()
    city = (data.get("city") or "").strip()
    district = (data.get("district") or "").strip()

    def _regions_to_crawl(province: str, city: str, district: str) -> List[str]:
        if not province or province not in reg_json:
            raise ValueError("省份无效或未选择")
        prov_dict = reg_json[province]
        if city and city != "all" and district and district != "all":
            return [district]
        if city and city != "all" and (district == "all" or not district):
            dists = prov_dict.get(city, [])
            return dists if dists else [city]
        if city == "all" or not city:
            regions = []
            for c, dists in prov_dict.items():
                regions.extend(dists if dists else [c])
            return regions
        return [city]

    queries = [q.strip() for q in (data.get("queries") or "").split(",") if q.strip()] or DEFAULT_QUERIES
    try:
        qps = float(data.get("qps", 2.0))
    except Exception:
        qps = 2.0
    city_limit = bool(data.get("city_limit", True))

    try:
        regions = _regions_to_crawl(province, city, district)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    summary = {"ok": True, "regions": regions, "inserted_or_updated": 0, "per_region": [], "errors": []}
    for reg in regions:
        try:
            stats = crawl_region(ak=ak, region=reg, queries=queries, qps=qps, city_limit=city_limit)
            summary["per_region"].append({"region": reg, **stats})
            summary["inserted_or_updated"] += stats["inserted_or_updated"]
        except Exception as e:
            summary["errors"].append({"region": reg, "error": str(e)})
    return jsonify(summary), (200 if not summary["errors"] else 207)

@app.route("/data")
def data_api():
    source_query = request.args.get("source_query")
    return jsonify(fetch_geojson(source_query=source_query))

@app.route("/categories")
def categories():
    return jsonify(list_categories())

@app.route("/export_csv")
def export_csv():
    geo = fetch_geojson()
    rows: List[Dict[str, Any]] = []
    for f in geo.get("features", []):
        prop = f.get("properties", {})
        r = {k: prop.get(k, "") for k in CSV_FIELDS}
        lng, lat = (f.get("geometry", {}).get("coordinates") or [None, None])
        r["lng"], r["lat"] = lng, lat
        rows.append(r)
    si = io.StringIO(); si.write("\ufeff")  # BOM for Excel
    w = csv.DictWriter(si, fieldnames=CSV_FIELDS); w.writeheader()
    for r in rows: w.writerow(r)
    data = si.getvalue().encode("utf-8")
    return Response(
        data,
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename=poi_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"}
    )

@app.route("/export_map_db")
def export_map_db():
    geo = fetch_geojson()
    html = render_template(
        "map_export.html",
        title="POI 地图（数据库）",
        center=[36.06, 103.83],
        zoom=11,
        geojson=json.dumps(geo, ensure_ascii=False)
    )
    bio = io.BytesIO(html.encode("utf-8"))
    return send_file(bio, as_attachment=True,
                     download_name=f"poi_map_db_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html",
                     mimetype="text/html; charset=utf-8")

@app.route("/map")
def map_page():
    return render_template("map.html")

LON_CAND = ["lon","lng","longitude","经度","x","LON","LNG","LONGITUDE"]
LAT_CAND = ["lat","latitude","纬度","y","LAT","LATITUDE"]
NAME_CAND = ["name","poi_name","名称","NAME","店名","poiName"]
CAT_CAND  = [
    "category","类别","分类","CATEGORY",
    "source_query","源关键词","关键词","query",
    "tag","标签","poi_tag",
    "type","poi_type","类型"
]


def _first_exist(cols: List[str], candidates: List[str]):
    low = {c.lower(): c for c in cols}
    for k in candidates:
        if k.lower() in low:
            return low[k.lower()]
    return None

def read_csv_safely(file_storage) -> pd.DataFrame:
    file_storage.stream.seek(0)
    try:
        return pd.read_csv(file_storage, low_memory=False)
    except Exception:
        pass
    file_storage.stream.seek(0)
    try:
        return pd.read_csv(file_storage, encoding="gbk", low_memory=False)
    except Exception:
        pass
    file_storage.stream.seek(0)
    content = file_storage.read()
    from csv import Sniffer
    try:
        dialect = Sniffer().sniff(content[:4096].decode("utf-8", errors="ignore"))
        return pd.read_csv(io.BytesIO(content), sep=dialect.delimiter, low_memory=False)
    except Exception:
        return pd.read_csv(io.BytesIO(content), sep=",", low_memory=False)

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    cols = list(df.columns)

    lon_col = _first_exist(cols, LON_CAND)
    lat_col = _first_exist(cols, LAT_CAND)
    if lon_col is None or lat_col is None:
        raise ValueError("未找到经纬度列（支持：lon/lng/longitude/经度 与 lat/latitude/纬度）")

    name_col  = _first_exist(cols, NAME_CAND)
    srcq_col  = _first_exist(cols, ["source_query","源关键词","关键词","query"])  # 唯一分类依据
    addr_col  = _first_exist(cols, ["address","地址"])
    tel_col   = _first_exist(cols, ["telephone","phone","tel","电话"])
    rate_col  = _first_exist(cols, ["overall_rating","rating","评分"])
    price_col = _first_exist(cols, ["price","人均","均价","avg_price"])

    keep_map = { "lon": lon_col, "lat": lat_col }
    if name_col:  keep_map["name"] = name_col
    if srcq_col:  keep_map["source_query"] = srcq_col
    if addr_col:  keep_map["address"] = addr_col
    if tel_col:   keep_map["telephone"] = tel_col
    if rate_col:  keep_map["overall_rating"] = rate_col
    if price_col: keep_map["price"] = price_col

    out = df[list(keep_map.values())].copy()
    out.columns = list(keep_map.keys())

    out = out.replace({np.nan: None})
    out = out.dropna(subset=["lon","lat"])
    out["lon"] = pd.to_numeric(out["lon"], errors="coerce")
    out["lat"] = pd.to_numeric(out["lat"], errors="coerce")
    out = out.dropna(subset=["lon","lat"])
    out = out[(out["lon"] >= -180) & (out["lon"] <= 180) & (out["lat"] >= -90) & (out["lat"] <= 90)]

    if "source_query" not in out.columns:
        out["source_query"] = "未知"

    return out

def _clean_json_value(v):
    if v is None:
        return None
    try:
        import pandas as pd
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, (int, float, str, bool)):
        return v
    return str(v)

def df_to_geojson(df: pd.DataFrame) -> Dict[str, Any]:
    wanted = {"name","category","source_query","address","telephone","overall_rating","price","tag","type"}
    features = []
    df = df.replace({np.nan: None})

    for _, r in df.iterrows():
        lon = _clean_json_value(r.get("lon"))
        lat = _clean_json_value(r.get("lat"))
        if lon is None or lat is None:
            continue

        props = {}
        for k in df.columns:
            if k in ("lon","lat"):
                continue
            if (k in wanted) or True:
                props[k] = _clean_json_value(r.get(k))

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]},
            "properties": props
        })

    geojson = {"type": "FeatureCollection", "features": features}
    return json.loads(json.dumps(geojson, ensure_ascii=False, allow_nan=False))

@app.route("/upload_csv", methods=["POST"])
def upload_csv():
    if "files" not in request.files:
        return jsonify({"ok": False, "msg": "未接收到文件（表单字段名应为 files）"}), 400
    files = request.files.getlist("files")
    if not files:
        return jsonify({"ok": False, "msg": "没有选择文件"}), 400

    frames, stats = [], []
    for f in files:
        if not f.filename.lower().endswith(".csv"):
            stats.append({"file": f.filename, "error": "文件后缀不是 .csv"})
            continue
        try:
            df = read_csv_safely(f)
            df_norm = normalize_df(df)
            frames.append(df_norm)
            stats.append({"file": f.filename, "rows": int(len(df_norm))})
        except Exception as e:
            stats.append({"file": f.filename, "error": str(e)})

    if not frames:
        return jsonify({"ok": False, "msg": "没有有效的CSV文件", "files": stats}), 400

    merged = pd.concat(frames, ignore_index=True)
    keys = [c for c in ("lon","lat","name","category") if c in merged.columns]
    merged = merged.drop_duplicates(subset=keys).reset_index(drop=True)

    geojson = df_to_geojson(merged)
    safe_geojson = json.loads(json.dumps(geojson, ensure_ascii=False, allow_nan=False))

    global LATEST_GEOJSON
    LATEST_GEOJSON = safe_geojson
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    (UPLOAD_DIR / f"merged_{ts}.geojson").write_text(json.dumps(safe_geojson, ensure_ascii=False), encoding="utf-8")

    center = [float(merged["lat"].mean()), float(merged["lon"].mean())] if len(merged) else [34.0,108.0]
    return jsonify({
        "ok": True,
        "files": stats,
        "total_points": len(safe_geojson["features"]),
        "center": center,
        "geojson": safe_geojson
    })

@app.route("/export_map", methods=["POST"])
def export_map():
    data = request.get_json(silent=True) or {}
    geojson = data.get("geojson") or LATEST_GEOJSON
    title   = data.get("title") or "POI 可视化地图"
    center  = data.get("center") or [34.0, 108.0]
    zoom    = int(data.get("zoom") or 7)

    if not geojson or "features" not in geojson:
        return jsonify({"ok": False, "msg": "没有可导出的数据，请先上传并可视化"}), 400

    html = render_template(
        "map_export.html",
        title=title,
        center=center,
        zoom=zoom,
        geojson=json.dumps(geojson, ensure_ascii=False)
    )
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = EXPORT_DIR / f"map_{ts}.html"
    out_path.write_text(html, encoding="utf-8")
    return send_file(out_path, as_attachment=True, download_name=out_path.name, mimetype="text/html; charset=utf-8")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
