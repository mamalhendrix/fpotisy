import os
import re
import glob
import json
import time
import sqlite3
import asyncio
import threading
import traceback
import urllib.parse

import httpx
from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.responses import StreamingResponse, FileResponse
from starlette.background import BackgroundTask
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, TIT2, TPE1, TPE2, TALB, TRCK, TDRC

app = FastAPI()

# --- DEBUG MIDDLEWARE ---
@app.middleware("http")
async def log_requests(request: Request, call_next):
    query_params = dict(request.query_params)
    print(f"\n[INSPECTOR] ➡️ {request.method} {request.url.path}")
    if query_params:
        print(f"[INSPECTOR] 🔍 Query Params: {json.dumps(query_params, indent=2)}")
    response = await call_next(request)
    print(f"[INSPECTOR] ⬅️ Status: {response.status_code} for {request.url.path}\n")
    return response

# --- CONFIGURATION ---
NAVIDROME_URL = os.getenv("NAVIDROME_URL", "http://navidrome:4533")
MUSIC_DIR = os.getenv("MUSIC_FOLDER", "/music")
ONE_API_TOKEN = os.getenv("ONE_API_TOKEN", "")
ONE_API_URL = "https://api.one-api.ir/deezer/v1"
QUALITY = os.getenv("PREFERRED_QUALITY", "320")

ITUNES_URL = "https://itunes.apple.com"
ITUNES_HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"}

_transport = httpx.AsyncHTTPTransport(local_address="0.0.0.0", retries=1) if os.getenv("FORCE_IPV4") == "1" else None
http = httpx.AsyncClient(
    timeout=httpx.Timeout(60.0, connect=20.0),
    follow_redirects=True,
    transport=_transport,
)

locks: dict[str, asyncio.Lock] = {}
_bg_tasks: set = set()
_cache: dict[str, tuple] = {}

# --- PERSISTENT SQLITE CACHE SETUP ---
CACHE_DIR = os.getenv("CACHE_DIR", "./proxy_cache")
try:
    os.makedirs(CACHE_DIR, exist_ok=True)
    _probe = os.path.join(CACHE_DIR, ".write-test")
    open(_probe, "w").close()
    os.remove(_probe)
except OSError:
    CACHE_DIR = "/tmp/proxy-cache"
    os.makedirs(CACHE_DIR, exist_ok=True)
    print(f"CACHE_DIR is not writable; using {CACHE_DIR}")

STALE_GRACE = 7 * 86400


class Store:
    def __init__(self, path: str):
        self.db = sqlite3.connect(path, check_same_thread=False, timeout=10)
        self.lock = threading.Lock()
        try:
            self.db.execute("PRAGMA journal_mode=WAL")
        except sqlite3.DatabaseError:
            pass
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute("CREATE TABLE IF NOT EXISTS kv (ns TEXT NOT NULL, k TEXT NOT NULL, v TEXT NOT NULL, "
                        "exp REAL NOT NULL, PRIMARY KEY (ns, k))")
        self.db.commit()
        self.pending: dict[tuple, tuple] = {}

    def get(self, ns: str, k: str):
        with self.lock:
            p = self.pending.get((ns, k))
            if p:
                return p
            row = self.db.execute("SELECT v, exp FROM kv WHERE ns=? AND k=?", (ns, k)).fetchone()
        return (json.loads(row[0]), row[1]) if row else None

    def put(self, ns: str, k: str, value, ttl: float):
        with self.lock:
            self.pending[(ns, k)] = (value, time.time() + ttl)

    def flush(self):
        with self.lock:
            items = list(self.pending.items())
        if not items:
            return
        rows = [(ns, k, json.dumps(v), exp) for (ns, k), (v, exp) in items]
        with self.lock:
            self.db.executemany("INSERT OR REPLACE INTO kv (ns, k, v, exp) VALUES (?,?,?,?)", rows)
            self.db.commit()
            for key, val in items:
                if self.pending.get(key) is val:
                    del self.pending[key]

    def purge(self):
        with self.lock:
            self.db.execute("DELETE FROM kv WHERE exp < ?", (time.time() - STALE_GRACE,))
            self.db.commit()
            n = self.db.execute("SELECT COUNT(*) FROM kv").fetchone()[0]
        print(f"Persistent cache ready at {CACHE_DIR}: {n} entries stored.")


store = Store(os.path.join(CACHE_DIR, "proxy_cache.sqlite"))


class PDict(dict):
    def __init__(self, ns: str, ttl: float = 30 * 86400):
        super().__init__()
        self.ns, self.ttl = ns, ttl

    def __setitem__(self, k, v):
        super().__setitem__(k, v)
        store.put(self.ns, k, v, self.ttl)

    def _load(self, k) -> bool:
        if dict.__contains__(self, k):
            return True
        hit = store.get(self.ns, k)
        if hit and hit[1] > time.time():
            dict.__setitem__(self, k, hit[0])
            return True
        return False

    def __missing__(self, k):
        if self._load(k):
            return dict.__getitem__(self, k)
        raise KeyError(k)

    def get(self, k, default=None):
        return dict.__getitem__(self, k) if self._load(k) else default

    def __contains__(self, k):
        return self._load(k)


track_mapping = PDict("track_mapping")
cover_urls = PDict("cover_urls")
album_cache = PDict("album_cache")
playlist_cache = PDict("playlist_cache")

# ---------- HELPER: FULL ALBUM BUILDER WITH DEEZER TRACKS ----------
async def fetch_full_deezer_album_payload(collection_id: str, album_id: str):
    cached_payload = store.get("album", album_id)
    if cached_payload and cached_payload[1] > time.time():
        return cached_payload[0]

    results = await itunes_lookup(id=collection_id, entity="song")
    songs, album_name, album_artist, artwork_url = [], None, None, None
    
    for item in results:
        if item.get("wrapperType") == "collection":
            album_name = item.get("collectionName")
            album_artist = item.get("artistName")
            artwork_url = (item.get("artworkUrl100") or "").replace("100x100bb", "600x600bb") or None
        elif item.get("wrapperType") == "track":
            t_name = item.get("trackName")
            t_artist = item.get("artistName", album_artist or "Unknown")
            t_num = item.get("trackNumber", 1)
            t_dur = (item.get("trackTimeMillis") or 0) // 1000
            
            # Search Deezer for this track to get a real streamable deezer ID!
            deezer_track_id = None
            try:
                search_res = await api_get(f"/search/track?q={urllib.parse.quote(f'{t_artist} {t_name}')}&limit=1")
                cands = search_res.get("result", {}).get("data", [])
                if cands:
                    dt = cands[0]
                    deezer_track_id = str(dt.get("id"))
                    songs.append(deezer_track_to_subsonic(dt))
            except Exception:
                pass
                
            if not artwork_url and item.get("artworkUrl100"):
                artwork_url = item["artworkUrl100"].replace("100x100bb", "600x600bb")

    songs.sort(key=lambda s: s.get("track", 0))

    if artwork_url:
        cover_urls[album_id] = artwork_url

    res_payload = {
        "id": album_id,
        "name": album_name or "Collection",
        "title": album_name or "Collection",
        "album": album_name or "Collection",
        "artist": album_artist or "Apple Music",
        "isDir": True,
        "coverArt": album_id,
        "songCount": len(songs),
        "duration": sum(s.get("duration", 0) for s in songs),
        "song": songs,
    }
    if songs:
        store.put("album", album_id, res_payload, 7 * 86400)
    return res_payload

async def cached_call(key: str, ttl: float, fetch):
    now = time.monotonic()
    hit = _cache.get(key)
    if hit and hit[0] > now:
        return hit[1]
    
    stored = store.get("api", key)
    if stored and stored[1] > time.time():
        _cache[key] = (now + min(900, stored[1] - time.time()), stored[0])
        return stored[0]
        
    try:
        d = await fetch()
    except HTTPException:
        if stored:
            return stored[0]
        raise
        
    store.put("api", key, d, ttl)
    if len(_cache) > 1500:
        _cache.clear()
    _cache[key] = (now + min(900, ttl), d)
    return d


async def _flush_loop():
    while True:
        await asyncio.sleep(3)
        try:
            await asyncio.to_thread(store.flush)
        except Exception as e:
            print(f"Cache flush failed: {e!r}")


@app.on_event("startup")
async def _startup():
    await asyncio.to_thread(store.purge)
    asyncio.create_task(_flush_loop())


@app.on_event("shutdown")
async def _shutdown():
    await asyncio.to_thread(store.flush)
    await http.aclose()


# ---------- ONE-API (DEEZER) ----------

async def api_get(path: str, retries: int = 3):
    async def fetch():
        last = None
        for attempt in range(retries):
            try:
                r = await http.get(f"{ONE_API_URL}{path}", headers={"one-api-token": ONE_API_TOKEN})
                r.raise_for_status()
                return r.json()
            except (httpx.TransportError, httpx.HTTPStatusError) as e:
                last = e
                await asyncio.sleep(1.5 * (attempt + 1))
        raise HTTPException(502, f"One-API unreachable: {last!r}")
    
    return await cached_call(f"oneapi:{path}", 86400, fetch)


def deezer_track_to_subsonic(t: dict) -> dict:
    t_id = f"deezer-{t.get('id')}"
    artist = t.get('artist', {}).get('name', 'Unknown Artist')
    title = t.get('title', 'Unknown Title')
    album_info = t.get('album', {})
    album = album_info.get('title', 'Deezer Album')
    album_id = f"deezer-album-{album_info.get('id', '0')}"

    track_mapping[t_id] = {
        "artist": artist, 
        "title": title, 
        "album": album,
        "albumId": album_id,
        "duration": t.get('duration', 0), 
        "track": t.get('track_position', 1),
        "deezer_id": str(t.get('id'))
    }
    
    artwork = album_info.get('cover_medium') or album_info.get('cover_xl') or ''
    if artwork:
        cover_urls[t_id] = artwork
        cover_urls[album_id] = artwork

    return {
        "id": t_id,
        "title": title,
        "artist": artist,
        "album": album,
        "albumId": album_id,
        "duration": t.get('duration', 0),
        "track": t.get('track_position', 1),
        "isDir": False,
        "type": "music",
        "suffix": "mp3",
        "contentType": "audio/mpeg",
        "coverArt": album_id
    }


# ---------- ITUNES HELPERS (FOR PLAYLISTS/ALBUMS) ----------

async def itunes_lookup(ttl: int = 86400, **params):
    key = "lookup:" + "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    async def fetch():
        r = await http.get(f"{ITUNES_URL}/lookup", params=params, headers=ITUNES_HEADERS)
        r.raise_for_status()
        return r.json().get("results", [])
    return await cached_call(key, ttl, fetch)


async def itunes_search(kind: str, query: str, limit: int = 10):
    cache_key = f"itunes_search:{kind}:{query.lower().strip()}:{limit}"
    async def fetch():
        entity_map = {"album": "album", "playlist": "album", "artist": "allArtist"}
        entity = entity_map.get(kind, "album")
        r = await http.get(
            f"{ITUNES_URL}/search",
            params={"term": query, "entity": entity, "limit": limit},
            headers=ITUNES_HEADERS,
        )
        r.raise_for_status()
        return r.json().get("results", [])
    return await cached_call(cache_key, 86400, fetch)


def itunes_album_to_subsonic(t: dict) -> dict:
    album_id = f"itunes-album-{t.get('collectionId')}"
    artist = t.get('artistName', 'Unknown Artist')
    album = t.get('collectionName', 'iTunes Album')
    album_cache[album_id] = {"id": album_id, "name": album, "artist": artist, "collectionId": t.get('collectionId')}

    artwork = t.get('artworkUrl100', '').replace('100x100bb', '600x600bb')
    if artwork:
        cover_urls[album_id] = artwork

    return {
        "id": album_id,
        "name": album,
        "title": album,
        "album": album,
        "artist": artist,
        "isDir": True,
        "coverArt": album_id,
        "songCount": t.get('trackCount', 0),
    }


def itunes_playlist_to_subsonic(p: dict) -> dict:
    pid = f"itunes-playlist-{p.get('collectionId') or p.get('artistId')}"
    title = f"[Playlist] {p.get('collectionName') or p.get('artistName', 'iTunes Playlist')}"
    artist = p.get('artistName', 'Apple Music')
    playlist_cache[pid] = {"id": pid, "name": title, "collectionId": p.get('collectionId')}
    
    artwork = t.get('artworkUrl100', '').replace('100x100bb', '600x600bb') if 't' in locals() else ''
    if artwork:
        cover_urls[pid] = artwork

    return {
        "id": pid,
        "name": title,
        "title": title,
        "album": title,
        "artist": artist,
        "isDir": True,
        "coverArt": pid,
        "songCount": p.get('songCount', p.get('trackCount', 0)),
    }


def norm(x) -> str:
    return "".join(c for c in (x or "").lower() if c.isalnum())


def tkey(title) -> str:
    return norm(re.sub(r"[\(\[].*?[\)\]]", "", title or "")) or norm(title)


def similar_name(a: str, b: str) -> bool:
    return a == b or (len(a) > 6 and len(b) > 6 and (a in b or b in a))


def _count(params: dict, name: str, default: int = 20) -> int:
    try:
        return max(10, min(int(params.get(name, default)), 50))
    except ValueError:
        return default


def subsonic_ok(**payload):
    body = {"subsonic-response": {"status": "ok", "version": "1.16.1", **payload}}
    return Response(content=json.dumps(body), media_type="application/json")


# ---------- DOWNLOAD ENGINE ----------

def find_local(deezer_id: str):
    pattern = os.path.join(glob.escape(MUSIC_DIR), "*", "*" + glob.escape(f"[{deezer_id}].mp3"))
    hits = glob.glob(pattern)
    return hits[0] if hits else None


async def fetch_file(url: str, dest: str, attempts: int = 6):
    tmp = dest + ".part"
    for attempt in range(attempts):
        pos = os.path.getsize(tmp) if os.path.exists(tmp) else 0
        headers = {"Range": f"bytes={pos}-"} if pos else {}
        try:
            async with http.stream("GET", url, headers=headers, timeout=httpx.Timeout(120.0, connect=30.0)) as r:
                if r.status_code == 416:
                    os.replace(tmp, dest)
                    return
                if r.status_code not in (200, 206):
                    raise httpx.HTTPStatusError("bad status", request=r.request, response=r)
                mode = "ab" if (pos and r.status_code == 206) else "wb"
                with open(tmp, mode) as f:
                    async for chunk in r.aiter_bytes(65536):
                        f.write(chunk)
            os.replace(tmp, dest)
            return
        except (httpx.TransportError, httpx.HTTPStatusError, asyncio.TimeoutError) as e:
            print(f"Download interrupted (attempt {attempt + 1}/{attempts}): {e!r}")
            await asyncio.sleep(3 * (attempt + 1))
    raise HTTPException(502, "Download from Deezer failed after retries")


async def trigger_scan(auth_params: dict):
    try:
        await http.get(f"{NAVIDROME_URL}/rest/startScan", params=auth_params)
    except Exception as e:
        print(f"Scan trigger failed: {e!r}")


async def ensure_local(song_id: str, auth_params: dict) -> str:
    meta = track_mapping.get(song_id)
    if not meta:
        raise HTTPException(404, f"Track mapping missing for: {song_id}")

    deezer_id = meta.get("deezer_id") or song_id.replace("deezer-", "")
    if not deezer_id.isdigit():
        raise HTTPException(400, f"Invalid Deezer ID format: {song_id}")

    existing = find_local(deezer_id)
    if existing:
        return existing

    lock = locks.setdefault(deezer_id, asyncio.Lock())
    async with lock:
        existing = find_local(deezer_id)
        if existing:
            return existing

        qualities_to_try = list(dict.fromkeys([QUALITY if QUALITY != "FLAC" else "320", "320", "128"]))
        url = None
        for q in qualities_to_try:
            try:
                dl = await api_get(f"/track/{deezer_id}/download/{q}")
                url = dl.get("result", {}).get("url")
                if url:
                    break
            except HTTPException:
                continue

        if not url:
            raise HTTPException(502, f"Could not find download stream for track ID {deezer_id}")

        artist, title = meta["artist"], meta["title"]
        safe_artist = "".join(x for x in artist if x.isalnum() or x in " -_") or "Unknown"
        safe_title = "".join(x for x in title if x.isalnum() or x in " -_") or "Track"
        folder = os.path.join(MUSIC_DIR, safe_artist)
        os.makedirs(folder, exist_ok=True)
        dest = os.path.join(folder, f"{safe_title} [{deezer_id}].mp3")

        print(f"Downloading {artist} - {title}")
        await fetch_file(url, dest)
        try:
            audio = MP3(dest, ID3=ID3)
            if audio.tags is None:
                audio.add_tags()
            audio.tags.add(TIT2(encoding=3, text=title))
            audio.tags.add(TPE1(encoding=3, text=artist))
            audio.tags.add(TPE2(encoding=3, text=artist))
            if meta.get("album"):
                audio.tags.add(TALB(encoding=3, text=meta["album"]))
            if meta.get("track"):
                audio.tags.add(TRCK(encoding=3, text=str(meta["track"])))
            audio.save()
        except Exception as e:
            print(f"Tagging failed (file still saved): {e}")

        t = asyncio.create_task(trigger_scan(auth_params))
        _bg_tasks.add(t)
        t.add_done_callback(_bg_tasks.discard)
        return dest


async def passthrough(endpoint: str, params: dict):
    req = http.build_request("GET", f"{NAVIDROME_URL}/rest/{endpoint}", params=params)
    res = await http.send(req, stream=True)
    headers = {k: v for k, v in res.headers.items()
               if k.lower() not in ("transfer-encoding", "connection")}
    return StreamingResponse(
        res.aiter_raw(),
        status_code=res.status_code,
        headers=headers,
        background=BackgroundTask(res.aclose),
    )

# ---------- HELPER: BUILD DEEZER ALBUM FROM DEEZER API ----------
async def fetch_deezer_album_payload(deezer_album_id: str, album_id: str):
    cached_payload = store.get("album", album_id)
    if cached_payload and cached_payload[1] > time.time():
        return cached_payload[0]

    try:
        res = await api_get(f"/album/{deezer_album_id}")
        data = res.get("result", {})
        album_name = data.get("title", "Album")
        artist_name = data.get("artist", {}).get("name", "Unknown Artist")
        artwork_url = data.get("cover_medium") or data.get("cover_xl") or ""
        
        tracks_data = data.get("tracks", {}).get("data", [])
        songs = []
        for t in tracks_data:
            # Ensure proper fields for deezer track conversion
            t["album"] = {"title": album_name, "id": deezer_album_id, "cover_medium": artwork_url}
            if not t.get("artist"):
                t["artist"] = {"name": artist_name}
            songs.append(deezer_track_to_subsonic(t))

        if artwork_url:
            cover_urls[album_id] = artwork_url

        res_payload = {
            "id": album_id,
            "name": album_name,
            "title": album_name,
            "album": album_name,
            "artist": artist_name,
            "isDir": True,
            "coverArt": album_id,
            "songCount": len(songs),
            "duration": sum(s.get("duration", 0) for s in songs),
            "song": songs,
        }
        if songs:
            store.put("album", album_id, res_payload, 7 * 86400)
        return res_payload
    except Exception as e:
        print(f"Failed to fetch Deezer album tracks: {e!r}")
        return None


# ---------- AUTH CHECK ----------

AUTH_TTL = 300
auth_cache: dict[tuple, float] = {}


async def is_authenticated(params: dict) -> bool:
    if "u" not in params:
        return False
    keys = ("u", "p", "t", "s")
    key = tuple(params.get(k) for k in keys)
    now = time.monotonic()
    if auth_cache.get(key, 0) > now:
        return True
    try:
        r = await http.get(
            f"{NAVIDROME_URL}/rest/ping",
            params={**{k: params[k] for k in keys if k in params}, "v": "1.16.1", "c": "proxy", "f": "json"},
        )
        ok = r.json().get("subsonic-response", {}).get("status") == "ok"
    except Exception as e:
        print(f"Auth check failed: {e!r}")
        return False
    if ok:
        auth_cache[key] = now + AUTH_TTL
    return ok


# ---------- ROUTES ----------

@app.get("/rest/{endpoint}")
async def subsonic_proxy(endpoint: str, request: Request):
    if endpoint.endswith(".view"):
        endpoint = endpoint[: -len(".view")]

    params = dict(request.query_params)

    if endpoint == "ping" and "u" not in params:
        return subsonic_ok()

    if not await is_authenticated(params):
        err = {"subsonic-response": {"status": "failed", "version": "1.16.1",
                                     "error": {"code": 40, "message": "Wrong username or password"}}}
        return Response(content=json.dumps(err), media_type="application/json")

# 1. SEARCH: Populate Artists & Tracks from Deezer
    if endpoint in ("search3", "search2"):
        local_res = await http.get(f"{NAVIDROME_URL}/rest/{endpoint}", params=params)
        data = local_res.json()
        sr = data.setdefault("subsonic-response", {}).setdefault(
            "searchResult3" if endpoint == "search3" else "searchResult2", {})
        query = params.get("query", "").strip().strip('"').strip()

        if query:
            local_songs = sr.get("song", [])
            local_albums = sr.get("album", [])
            local_artists = sr.get("artist", [])

            seen_songs = {(norm(x.get("artist")), norm(x.get("title"))) for x in local_songs}
            seen_artists = {norm(x.get("name")) for x in local_artists}

            try:
                deezer_res = await api_get(f"/search/track?q={urllib.parse.quote(query)}&limit=30")
                deezer_tracks = deezer_res.get("result", {}).get("data", [])
                extra_songs = [deezer_track_to_subsonic(t) for t in deezer_tracks]
                extra_songs = [x for x in extra_songs if (norm(x["artist"]), norm(x["title"])) not in seen_songs]
                sr["song"] = local_songs + extra_songs

                # Build artist list from Deezer results so artists appear in search!
                extra_artists = []
                for t in deezer_tracks:
                    art_info = t.get("artist", {})
                    art_name = art_info.get("name")
                    art_id = f"deezer-artist-{art_info.get('id')}"
                    if art_name and norm(art_name) not in seen_artists:
                        seen_artists.add(norm(art_name))
                        cover = art_info.get("picture_medium") or art_info.get("picture_xl")
                        if cover:
                            cover_urls[art_id] = cover
                        extra_artists.append({
                            "id": art_id,
                            "name": art_name,
                            "coverArt": art_id if cover else None,
                            "albumCount": 1
                        })
                sr["artist"] = local_artists + extra_artists
            except Exception as e:
                print(f"Deezer search failed: {e!r}")

            try:
                albums, playlists = await asyncio.gather(
                    itunes_search("album", query, _count(params, "albumCount")),
                    itunes_search("playlist", query, 5),
                    return_exceptions=True,
                )
                if not isinstance(albums, Exception):
                    sr["album"] = local_albums + [itunes_album_to_subsonic(a) for a in albums]
                if not isinstance(playlists, Exception):
                    pl_extra = [itunes_playlist_to_subsonic(p) for p in playlists if p.get('collectionId')]
                    sr["album"] = sr.get("album", local_albums) + pl_extra
            except Exception:
                pass

            sr["songCount"] = len(sr.get("song", []))
            sr["albumCount"] = len(sr.get("album", []))
            sr["artistCount"] = len(sr.get("artist", []))

        return Response(content=json.dumps(data), media_type="application/json")
        
    # 1d. GETARTIST: Intercept artist requests and fetch full albums from Deezer/iTunes
    if endpoint == "getArtist":
        artist_id = params.get("id", "")
        
        cached_payload = store.get("artist_payload", artist_id)
        if cached_payload and cached_payload[1] > time.time():
            return subsonic_ok(artist=cached_payload[0])

        artist_name = "Unknown Artist"
        if artist_id.startswith("deezer-artist-"):
            art_num = artist_id.replace("deezer-artist-", "")
            try:
                res = await api_get(f"/artist/{art_num}")
                artist_name = res.get("result", {}).get("name", "Unknown Artist")
            except Exception:
                pass

        # Fallback to local Navidrome search or track mapping if name not found
        nav_albums = []
        try:
            nav_res = await http.get(f"{NAVIDROME_URL}/rest/getArtist", params={**params, "f": "json"})
            nav_data = nav_res.json().get("subsonic-response", {}).get("artist", {})
            if isinstance(nav_data, dict):
                if artist_name == "Unknown Artist":
                    artist_name = nav_data.get("name", "Unknown Artist")
                nav_albums = nav_data.get("album", [])
        except Exception:
            pass

        # Fetch full artist albums from Deezer
        albums = []
        seen_albums = set()
        try:
            if artist_name != "Unknown Artist":
                d_res = await api_get(f"/search/album?q={urllib.parse.quote(artist_name)}&limit=50")
                for d_album in d_res.get("result", {}).get("data", []):
                    d_art_name = d_album.get("artist", {}).get("name", "")
                    if norm(d_art_name) == norm(artist_name) or artist_name.lower() in d_art_name.lower():
                        alb_id = f"deezer-album-{d_album.get('id')}"
                        alb_title = d_album.get("title", "Album")
                        if norm(alb_title) not in seen_albums:
                            seen_albums.add(norm(alb_title))
                            cover = d_album.get("cover_medium") or d_album.get("cover_xl")
                            if cover:
                                cover_urls[alb_id] = cover
                            albums.append({
                                "id": alb_id,
                                "name": alb_title,
                                "title": alb_title,
                                "album": alb_title,
                                "artist": artist_name,
                                "isDir": True,
                                "coverArt": alb_id if cover else None,
                                "songCount": d_album.get("nb_tracks", 0)
                            })
        except Exception as e:
            print(f"Deezer artist albums fetch failed: {e!r}")

        # Merge with any local albums found
        if nav_albums:
            have = {norm(a.get("name")) for a in albums}
            for na in nav_albums:
                if norm(na.get("name")) not in have:
                    albums.append(na)

        artist_cover = cover_urls.get(artist_id) or (albums[0].get("coverArt") if albums else None)
        if artist_cover:
            cover_urls[artist_id] = artist_cover

        res_payload = {
            "id": artist_id,
            "name": artist_name,
            "coverArt": artist_id if artist_cover else None,
            "albumCount": len(albums),
            "album": albums
        }

        if albums:
            store.put("artist_payload", artist_id, res_payload, 6 * 3600)
            return subsonic_ok(artist=res_payload)
        
    # 2. GETSONG handling for Deezer IDs
    if endpoint == "getSong" and params.get("id", "").startswith("deezer-"):
        song_id = params["id"]
        meta = track_mapping.get(song_id, {"artist": "Unknown", "title": "Track"})
        dummy = {"subsonic-response": {
            "status": "ok", "version": "1.16.1",
            "song": {"id": song_id, "title": meta["title"], "artist": meta["artist"],
                     "album": meta.get("album") or "Deezer Track", "duration": meta.get("duration", 0),
                     "albumId": meta.get("albumId"), "coverArt": meta.get("albumId")},
        }}
        return Response(content=json.dumps(dummy), media_type="application/json")

    # 3. STREAM & DOWNLOAD for Deezer IDs
    if endpoint in ("stream", "download") and params.get("id", "").startswith("deezer-"):
        song_id = params["id"]
        auth_params = {k: v for k, v in params.items() if k in ["u", "p", "t", "s", "v", "c", "f"]}
        try:
            path = await ensure_local(song_id, auth_params)
            return FileResponse(path, media_type="audio/mpeg")
        except Exception as e:
            print(f"[STREAM CRITICAL EXCEPTION] Failed for song {song_id}: {e!r}")
            traceback.print_exc()
            raise HTTPException(502, f"Stream preparation failed: {e!r}")
    if endpoint == "getAlbum":
        target_id = params.get("id", "")
        
        if target_id.startswith("deezer-album-"):
            deezer_album_id = target_id.replace("deezer-album-", "")
            res_payload = await fetch_deezer_album_payload(deezer_album_id, target_id)
            if res_payload:
                return subsonic_ok(album=res_payload)

        if target_id.startswith(("itunes-album-", "itunes-playlist-")):
            collection_id = target_id.replace("itunes-album-", "").replace("itunes-playlist-", "")
            res_payload = await fetch_full_deezer_album_payload(collection_id, target_id)
            return subsonic_ok(album=res_payload)
    
    # 1c. COVER ART handling with persistent local cache
    if endpoint == "getCoverArt":
        item_id = params.get("id", "")
        
        # 1. Check if cached in SQLite
        cached_record = store.get("cover_blob", item_id)
        if cached_record and cached_record[1] > time.time():
            blob, c_type = cached_record[0]
            return Response(content=blob, media_type=c_type, headers={"Cache-Control": "public, max-age=604800"})

        url = cover_urls.get(item_id)
        
        # 2. Fallback resolution for Deezer/iTunes IDs if URL isn't cached yet
        if not url:
            if item_id.startswith("deezer-album-"):
                try:
                    res = await api_get(f"/album/{item_id.replace('deezer-album-', '')}")
                    data = res.get("result", {})
                    url = data.get("cover_medium") or data.get("cover_xl")
                    if url:
                        cover_urls[item_id] = url
                except Exception:
                    pass
            elif item_id.startswith("itunes-album-"):
                try:
                    results = await itunes_lookup(id=item_id.replace("itunes-album-", ""))
                    for item in results:
                        art = (item.get("artworkUrl100") or "").replace("100x100bb", "600x600bb")
                        if art:
                            url = art
                            cover_urls[item_id] = art
                            break
                except Exception:
                    pass

        # 3. Download, cache locally in SQLite, and return
        if url:
            try:
                r = await http.get(url, headers=ITUNES_HEADERS)
                if r.status_code == 200:
                    blob = r.content
                    c_type = r.headers.get("content-type", "image/jpeg")
                    store.put("cover_blob", item_id, (blob, c_type), 30 * 86400)
                    return Response(content=blob, media_type=c_type, headers={"Cache-Control": "public, max-age=604800"})
            except Exception as e:
                print(f"Failed to fetch cover art from URL {url}: {e!r}")
        
        return await passthrough(endpoint, params)

    # 4. DEFAULT PASSTHROUGH TO NAVIDROME
    return await passthrough(endpoint, params)
