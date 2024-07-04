import asyncio
import logging
import os
import re
import shutil
import time
import zipfile
import requests
import starlette.status as status

from aiocron import crontab
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.responses import FileResponse
from cachetools import TTLCache

from debrid.get_debrid_service import get_debrid_service
from jackett.jackett_result import JackettResult
from jackett.jackett_service import JackettService
from zilean.zilean_result import ZileanResult
from zilean.zilean_service import ZileanService
from metdata.cinemeta import Cinemeta
from metdata.tmdb import TMDB
from torrent.torrent_service import TorrentService
from torrent.torrent_smart_container import TorrentSmartContainer
from utils.cache import search_redis, cache_redis, cache_public, search_public
from utils.filter_results import filter_items, sort_items, merge_items
from utils.logger import setup_logger
from utils.parse_config import parse_config
from utils.stremio_parser import parse_to_stremio_streams
from utils.string_encoding import decodeb64

load_dotenv()

logger = setup_logger(__name__)

root_path = os.environ.get("ROOT_PATH", None)
if root_path and not root_path.startswith("/"):
    root_path = "/" + root_path

app = FastAPI(root_path=root_path)

VERSION = "4.1.6"
isDev = os.getenv("NODE_ENV") == "development"

logger.info(f"Initializing Jackett Addon v{VERSION}")


class LogFilterMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        request = Request(scope, receive)
        path = request.url.path
        sensible_path = re.sub(r"/ey.*?/", "//", path)
        logger.info(f"Incoming request: {request.method} - {sensible_path}")
        return await self.app(scope, receive, send)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if not isDev:
    app.add_middleware(LogFilterMiddleware)

templates = Jinja2Templates(directory="templates")
cache = TTLCache(maxsize=100, ttl=3600)


@app.get("/")
async def root():
    logger.info("Redirecting to /configure")
    return RedirectResponse(url="/configure")


@app.get("/configure")
@app.get("/{config}/configure")
async def configure(request: Request):
    logger.info("Serving configuration page")
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/static/{file_path:path}")
async def serve_static(file_path: str):
    logger.debug(f"Serving static file: {file_path}")
    return FileResponse(f"templates/{file_path}")


@app.get("/manifest.json")
@app.get("/{params}/manifest.json")
async def get_manifest():
    logger.info("Serving manifest.json")
    return {
        "id": "community.aymene69.jackett",
        "icon": "https://i.imgur.com/tVjqEJP.png",
        "version": VERSION,
        "catalogs": [],
        "resources": ["stream"],
        "types": ["movie", "series"],
        "name": "Jackett" + (" (Dev)" if isDev else ""),
        "description": "Elevate your Stremio experience with seamless access to Jackett torrent links, effortlessly "
        "fetching torrents for your selected movies within the Stremio interface.",
        "behaviorHints": {
            "configurable": True,
        },
    }


@app.get("/{config}/stream/{stream_type}/{stream_id}")
async def get_results(config: str, stream_type: str, stream_id: str, request: Request):
    start = time.time()
    logger.info(f"Stream request: {stream_type} - {stream_id}")

    stream_id = stream_id.replace(".json", "")
    config = parse_config(config)
    logger.debug(f"Parsed configuration: {config}")

    logger.info(f"Fetching metadata from {config['metadataProvider']}")
    if config["metadataProvider"] == "tmdb" and config["tmdbApi"]:
        metadata_provider = TMDB(config)
    else:
        metadata_provider = Cinemeta(config)

    media = metadata_provider.get_metadata(stream_id, stream_type)
    logger.info(f"Retrieved media metadata: {str(media.titles)}")

    debrid_service = get_debrid_service(config)
    search_results = []
    torrent_service = TorrentService()

    if config["cache"]:
        logger.info("Searching local cache")
        local_cached_results = search_redis(media)
        if local_cached_results:
            logger.info(f"Found {len(local_cached_results)} local cached results")
        else:
            logger.info("No local cached results found")

        logger.info("Searching public cache")
        public_cached_results = search_public(media)
        if public_cached_results:
            logger.info(f"Found {len(public_cached_results)} public cached results")
            public_cached_results = [
                JackettResult().from_cached_item(torrent, media)
                for torrent in public_cached_results
                if len(torrent.get("hash", "")) == 40
            ]
            public_cached_results = filter_items(
                public_cached_results, media, config=config
            )
            public_cached_results = torrent_service.convert_and_process(
                public_cached_results
            )
        else:
            logger.info("No public cached results found")

        search_results = local_cached_results + public_cached_results
        logger.info(f"Total cached results: {len(search_results)}")

    if config["zilean"] and len(search_results) < int(config["minCachedResults"]):
        logger.info("Searching Zilean API")
        zilean_service = ZileanService(config)
        zilean_search_results = zilean_service.search(media)
        if zilean_search_results:
            logger.info(f"Found {len(zilean_search_results)} results from Zilean")
            zilean_search_results = [
                ZileanResult().from_api_cached_item(torrent, media)
                for torrent in zilean_search_results
                if len(torrent.get("infoHash", "")) == 40
            ]
            zilean_search_results = filter_items(
                zilean_search_results, media, config=config
            )
            zilean_search_results = torrent_service.convert_and_process(
                zilean_search_results
            )
            search_results = merge_items(search_results, zilean_search_results)
        else:
            logger.info("No results found on Zilean API")

    if config["jackett"] and len(search_results) < int(config["minCachedResults"]):
        logger.info("Searching Jackett")
        jackett_service = JackettService(config)
        jackett_search_results = jackett_service.search(media)
        logger.info(f"Found {len(jackett_search_results)} results from Jackett")

        filtered_jackett_search_results = filter_items(
            jackett_search_results, media, config=config
        )
        logger.info(f"Filtered Jackett results: {len(filtered_jackett_search_results)}")

        if filtered_jackett_search_results:
            torrent_results = torrent_service.convert_and_process(
                filtered_jackett_search_results
            )
            logger.debug(
                f"Converted {len(torrent_results)} Jackett results to TorrentItems"
            )
            search_results = merge_items(search_results, torrent_results)

    if config["cache"] and search_results:
        logger.debug(f"Caching {len(search_results)} TorrentItems")
        cache_redis(search_results, media)

    logger.debug("Creating TorrentSmartContainer")
    torrent_smart_container = TorrentSmartContainer(search_results, media)

    if config["debrid"]:
        logger.info("Checking debrid availability")
        hashes = torrent_smart_container.get_hashes()
        ip = request.client.host
        result = debrid_service.get_availability_bulk(hashes, ip)
        torrent_smart_container.update_availability(result, type(debrid_service), media)
        logger.info(f"Checked availability for {len(result.items())} items")

    if config["cache"]:
        logger.debug("Caching public container items")
        torrent_smart_container.cache_container_items()

    logger.info("Getting best matching results")
    best_matching_results = torrent_smart_container.get_best_matching()
    best_matching_results = sort_items(best_matching_results, config)
    logger.info(f"Found {len(best_matching_results)} best matching results")

    logger.info("Processing results for Stremio")
    stream_list = parse_to_stremio_streams(best_matching_results, config, media)
    logger.info(f"Processed {len(stream_list)} streams for Stremio")

    total_time = time.time() - start
    logger.info(f"Request completed in {total_time:.2f} seconds")

    return {"streams": stream_list}


def get_cached_stream_link(query: str, config: dict, ip: str):
    cache_key = f"{query}_{ip}"
    if cache_key not in cache:
        logger.debug(f"Cache miss for {cache_key}")
        debrid_service = get_debrid_service(config)
        cache[cache_key] = debrid_service.get_stream_link(query, config, ip)
    else:
        logger.debug(f"Cache hit for {cache_key}")
    return cache[cache_key]


@app.get("/playback/{config}/{query}")
async def get_playback(config: str, query: str, request: Request):
    try:
        if not query:
            raise HTTPException(status_code=400, detail="Query required.")

        config = parse_config(config)
        decoded_query = decodeb64(query)
        ip = request.client.host

        logger.info(f"Playback request: {decoded_query}")
        link = get_cached_stream_link(decoded_query, config, ip)
        logger.info(f"Stream link generated: {link}")

        return RedirectResponse(url=link, status_code=status.HTTP_301_MOVED_PERMANENTLY)
    except Exception as e:
        logger.error(f"Playback error: {e}")
        raise HTTPException(
            status_code=500, detail="An error occurred while processing the request."
        )


@app.head("/playback/{config}/{query}")
async def head_playback(config: str, query: str, request: Request):
    try:
        if not query:
            raise HTTPException(status_code=400, detail="Query required.")

        config = parse_config(config)
        decoded_query = decodeb64(query)
        ip = request.client.host
        cache_key = f"{decoded_query}_{ip}"

        if cache_key in cache:
            logger.debug(f"HEAD request: Cache hit for {cache_key}")
            return Response(status_code=status.HTTP_200_OK)
        else:
            logger.debug(f"HEAD request: Cache miss for {cache_key}")
            time.sleep(0.1)
            return Response(status_code=status.HTTP_200_OK)
    except Exception as e:
        logger.error(f"HEAD request error: {e}")
        raise HTTPException(
            status_code=500, detail="An error occurred while processing the request."
        )


async def update_app():
    try:
        current_version = "v" + VERSION
        url = "https://api.github.com/repos/aymene69/stremio-jackett/releases/latest"
        response = requests.get(url)
        data = response.json()
        latest_version = data["tag_name"]

        if latest_version != current_version:
            logger.info(f"New version available: {latest_version}")
            logger.info("Starting update process")

            logger.info("Downloading update zip")
            update_zip = requests.get(data["zipball_url"])
            with open("update.zip", "wb") as file:
                file.write(update_zip.content)
            logger.info("Update zip downloaded")

            logger.info("Extracting update")
            with zipfile.ZipFile("update.zip", "r") as zip_ref:
                zip_ref.extractall("update")
            logger.info("Update extracted")

            extracted_folder = os.listdir("update")[0]
            extracted_folder_path = os.path.join("update", extracted_folder)
            for item in os.listdir(extracted_folder_path):
                s = os.path.join(extracted_folder_path, item)
                d = os.path.join(".", item)
                if os.path.isdir(s):
                    shutil.copytree(s, d, dirs_exist_ok=True)
                else:
                    shutil.copy2(s, d)
            logger.info("Files updated")

            logger.info("Cleaning up")
            shutil.rmtree("update")
            os.remove("update.zip")
            logger.info("Update completed successfully")
        else:
            logger.info("Application is up to date")
    except Exception as e:
        logger.error(f"Update error: {e}")


@crontab("* * * * *", start=not isDev)
async def schedule_task():
    # await update_app()
    pass


async def main():
    await asyncio.gather(schedule_task())


if __name__ == "__main__":
    logger.info("Starting Jackett Addon")
    asyncio.run(main())
