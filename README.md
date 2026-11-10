# Subsonic Deezer & iTunes Proxy

A high-performance FastAPI proxy service that bridges **Navidrome** with **Deezer** (via One-API) and **Apple Music/iTunes**, allowing you to search, browse, and stream a massive global music catalog seamlessly inside any Subsonic-compatible client (like Tempus, DSub, or Symfonium).

---

## 🚀 Features

* **Hybrid Catalog Architecture:** Uses Deezer for direct, high-quality audio streaming and track resolution, while leveraging iTunes/Apple Music for rich album art, playlists, and discography browsing.
* **Persistent SQLite Caching:** Caches API responses, track mappings, and album cover blobs locally to minimize external network overhead and bypass provider restrictions.
* **Smart Track Fallback & Matching:** Automatically strips extraneous tags and tests multiple query variations to ensure high-accuracy track matching between iTunes metadata and Deezer audio streams.
* **Automatic Local Archiving:** Automatically downloads streamed tracks into your local music library directory and triggers Navidrome background scans.

---

## 📁 Project Structure

├── README.md
├── docker-compose.yml
├── data/              # Persistent data volume
├── proxy/
│   ├── Dockerfile
│   └── main.py        # Core FastAPI proxy application
└── proxy-cache/       # Persistent SQLite cache storage

## if you use windows WSL , use extfat format for music directory
# I use https://one-api.ir/ for deezer api , you can use other providers that you know.
# HOW TO USE:
git clone git@github.com:mamalhendrix/fpotisy.git
cd fpotisy
docker compose up -d --build
