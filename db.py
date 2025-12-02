import math
import sqlite3
from pathlib import Path
from typing import Iterable, Dict, Any

DB_PATH = Path("./poi.sqlite")

DDL = """
CREATE TABLE IF NOT EXISTS poi (
  uid TEXT PRIMARY KEY,
  name TEXT,
  address TEXT,
  province TEXT,
  city TEXT,
  area TEXT,
  adcode TEXT,
  lat REAL,
  lng REAL,
  type TEXT,
  tag TEXT,
  classified_poi_tag TEXT,
  telephone TEXT,
  detail INTEGER,
  overall_rating TEXT,
  price TEXT,
  shop_hours TEXT,
  brand TEXT,
  content_tag TEXT,
  source_query TEXT
);
"""

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(DDL)
    return conn

def upsert_rows(rows: Iterable[Dict[str, Any]]):
    conn = get_conn()
    with conn:
        conn.executemany("""
        INSERT INTO poi(uid,name,address,province,city,area,adcode,lat,lng,type,tag,classified_poi_tag,
                        telephone,detail,overall_rating,price,shop_hours,brand,content_tag,source_query)
        VALUES (:uid,:name,:address,:province,:city,:area,:adcode,:lat,:lng,:type,:tag,:classified_poi_tag,
                :telephone,:detail,:overall_rating,:price,:shop_hours,:brand,:content_tag,:source_query)
        ON CONFLICT(uid) DO UPDATE SET
          name=excluded.name,
          address=excluded.address,
          province=excluded.province,
          city=excluded.city,
          area=excluded.area,
          adcode=excluded.adcode,
          lat=excluded.lat,
          lng=excluded.lng,
          type=excluded.type,
          tag=excluded.tag,
          classified_poi_tag=excluded.classified_poi_tag,
          telephone=excluded.telephone,
          detail=excluded.detail,
          overall_rating=excluded.overall_rating,
          price=excluded.price,
          shop_hours=excluded.shop_hours,
          brand=excluded.brand,
          content_tag=excluded.content_tag,
          source_query=excluded.source_query
        """, list(rows))
    conn.close()

PI = math.pi
AXIS = 6378245.0
EE = 0.00669342162296594323
def _out_of_china(lng, lat):
    return not (72.004 <= lng <= 137.8347 and 0.8293 <= lat <= 55.8271)
def _transform_lat(lng, lat):
    ret = -100.0 + 2.0*lng + 3.0*lat + 0.2*lat*lat + 0.1*lng*lat + 0.2*math.sqrt(abs(lng))
    ret += (20.0*math.sin(6.0*lng*PI) + 20.0*math.sin(2.0*lng*PI))*2.0/3.0
    ret += (20.0*math.sin(lat*PI) + 40.0*math.sin(lat/3.0*PI))*2.0/3.0
    ret += (160.0*math.sin(lat/12.0*PI) + 320.0*math.sin(lat*PI/30.0))*2.0/3.0
    return ret
def _transform_lng(lng, lat):
    ret = 300.0 + lng + 2.0*lat + 0.1*lng*lng + 0.1*lng*lat + 0.1*math.sqrt(abs(lng))
    ret += (20.0*math.sin(6.0*lng*PI) + 20.0*math.sin(2.0*lng*PI))*2.0/3.0
    ret += (20.0*math.sin(lng*PI) + 40.0*math.sin(lng/3.0*PI))*2.0/3.0
    ret += (150.0*math.sin(lng/12.0*PI) + 300.0*math.sin(lng/30.0*PI))*2.0/3.0
    return ret
def gcj02_to_wgs84(lng, lat):
    if lng is None or lat is None:
        return None, None
    if _out_of_china(lng, lat):
        return lng, lat
    dlat = _transform_lat(lng - 105.0, lat - 35.0)
    dlng = _transform_lng(lng - 105.0, lat - 35.0)
    radlat = lat / 180.0 * PI
    magic = math.sin(radlat)
    magic = 1 - EE * magic * magic
    sqrtMagic = math.sqrt(magic)
    dlat = (dlat * 180.0) / ((AXIS * (1 - EE)) / (magic * sqrtMagic) * PI)
    dlng = (dlng * 180.0) / (AXIS / sqrtMagic * math.cos(radlat) * PI)
    mgLat = lat + dlat
    mgLng = lng + dlng
    return (lng * 2 - mgLng, lat * 2 - mgLat)

def fetch_geojson(source_query: str = None):
    conn = get_conn()
    cur = conn.cursor()
    if source_query:
        cur.execute("""SELECT * FROM poi
                       WHERE lat IS NOT NULL AND lng IS NOT NULL AND source_query=?""", (source_query,))
    else:
        cur.execute("""SELECT * FROM poi
                       WHERE lat IS NOT NULL AND lng IS NOT NULL""")
    cols = [c[0] for c in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()

    feats = []
    for r in rows:
        lng_gcj, lat_gcj = r["lng"], r["lat"]
        lng_wgs, lat_wgs = gcj02_to_wgs84(lng_gcj, lat_gcj)
        feats.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lng_wgs, lat_wgs]},
            "properties": {k: r[k] for k in r if k not in ("lng","lat")}
        })
    return {"type": "FeatureCollection", "features": feats}

def list_categories():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT source_query, COUNT(*) FROM poi GROUP BY source_query ORDER BY COUNT(*) DESC")
    rows = [{"source_query": r[0], "count": r[1]} for r in cur.fetchall()]
    conn.close()
    return rows
