import asyncio
import hashlib
import logging
import os
import re
import shutil
import time
import zipfile
import requests
import starlette.status as status
import aiohttp

from aiocron import crontab
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException, Response, status, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from starlette.responses import FileResponse
from functools import lru_cache
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
from cache.local_redis import RedisCache
from utils.cache import search_public
from utils.filter_results import (
    filter_items,
    sort_items,
    merge_items,
    filter_out_non_matching_series,
    filter_out_non_matching_movies,
)
from utils.logger import setup_logger
from utils.parse_config import parse_config
from utils.stremio_parser import parse_to_stremio_streams
from utils.string_encoding import decodeb64
from models.movie import Movie
from models.series import Series

load_dotenv()

logger = setup_logger(__name__)

root_path = os.environ.get("ROOT_PATH", None)
if root_path and not root_path.startswith("/"):
    root_path = "/" + root_path


@lru_cache()
def get_redis_config():
    return {
        "redisHost": os.getenv("REDIS_HOST", "redis"),
        "redisPort": int(os.getenv("REDIS_PORT", 6379)),
        "redisExpiration": int(os.getenv("REDIS_EXPIRATION", 604800)),
    }


redis_cache = RedisCache(get_redis_config())

app = FastAPI(root_path=root_path)


def get_redis_cache():
    return redis_cache


VERSION = "5.0.0"
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
stream_cache = TTLCache(maxsize=1000, ttl=3600)


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


@app.get("/{config}/stream/{stream_type}/{stream_id}", response_model=None)
async def get_results(
    config: str,
    stream_type: str,
    stream_id: str,
    request: Request,
    redis_cache: RedisCache = Depends(get_redis_cache),
):
    start = time.time()
    logger.info(f"Stream request: {stream_type} - {stream_id}")

    stream_id = stream_id.replace(".json", "")
    config = parse_config(config)
    logger.debug(f"Parsed configuration: {config}")

    def get_metadata():
        logger.info(f"Fetching metadata from {config['metadataProvider']}")
        if config["metadataProvider"] == "tmdb" and config["tmdbApi"]:
            metadata_provider = TMDB(config)
        else:
            metadata_provider = Cinemeta(config)
        return metadata_provider.get_metadata(stream_id, stream_type)

    media = redis_cache.get_or_set(
        get_metadata, stream_id, stream_type, config["metadataProvider"]
    )
    logger.info(f"Retrieved media metadata: {str(media.titles)}")

    def cache_key(media):
        if isinstance(media, Movie):
            key_string = f"stream:{media.titles[0]}:{media.languages[0]}"
        elif isinstance(media, Series):
            key_string = f"stream:{media.titles[0]}:{media.languages[0]}:{media.season}{media.episode}"
        else:
            raise TypeError("Only Movie and Series are allowed as media!")
        hashed_key = hashlib.sha256(key_string.encode("utf-8")).hexdigest()
        return hashed_key[:16]

    cached_result = redis_cache.get(cache_key(media))
    if cached_result is not None:
        logger.info("Returning cached processed results")
        total_time = time.time() - start
        logger.info(f"Request completed in {total_time:.2f} seconds")
        return {"streams": cached_result}

    debrid_service = get_debrid_service(config)

    def filter_series_results(results, media):
        if media.type == "series":
            filtered = filter_out_non_matching_series(results, media.season, media.episode)
            logger.info(
                f"Filtered series results: {len(filtered)} (from {len(results)})"
            )
            return filtered
        else:
            filtered = filter_out_non_matching_movies(results, media.year)
            logger.info(
                f"Filtered movie results: {len(filtered)} (from {len(results)})")
        return results

    def get_search_results(media, config):
        search_results = []
        torrent_service = TorrentService()

        def perform_search(update_cache=False):
            nonlocal search_results
            search_results = []

            if config["cache"] and not update_cache:
                public_cached_results = search_public(media)
                if public_cached_results:
                    logger.info(
                        f"Found {len(public_cached_results)} public cached results"
                    )
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
                    search_results.extend(public_cached_results)

            if config["zilean"] and len(search_results) < int(
                config["minCachedResults"]
            ):
                zilean_service = ZileanService(config)
                zilean_search_results = zilean_service.search(media)
                if zilean_search_results:
                    logger.info(
                        f"Found {len(zilean_search_results)} results from Zilean"
                    )
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

            if config["jackett"] and len(search_results) < int(
                config["minCachedResults"]
            ):
                jackett_service = JackettService(config)
                jackett_search_results = jackett_service.search(media)
                logger.info(f"Found {len(jackett_search_results)} results from Jackett")
                filtered_jackett_search_results = filter_items(
                    jackett_search_results, media, config=config
                )
                logger.info(
                    f"Filtered Jackett results: {len(filtered_jackett_search_results)}"
                )
                if filtered_jackett_search_results:
                    torrent_results = torrent_service.convert_and_process(
                        filtered_jackett_search_results
                    )
                    logger.debug(
                        f"Converted {len(torrent_results)} Jackett results to TorrentItems"
                    )
                    search_results = merge_items(search_results, torrent_results)

            if update_cache and search_results:
                logger.info(f"Updating cache with {len(search_results)} results")
                try:
                    redis_cache.set(redis_cache.generate_key(media), search_results)
                except Exception as e:
                    logger.error(f"Error updating cache: {e}")

        perform_search()
        return search_results

    def get_and_filter_results(media, config):
        min_results = int(config.get("minCachedResults", 1))
        cache_key = redis_cache.generate_key(get_search_results.__name__, media, config)

        unfiltered_results = redis_cache.get(cache_key)
        if unfiltered_results is None:
            logger.info("No results in cache. Performing new search.")
            nocache_results = get_search_results(media, config)
            redis_cache.set(cache_key, nocache_results)
            return nocache_results
        else:
            logger.info("Results retrieved from redis cache.")

        filtered_results = filter_series_results(unfiltered_results, media)

        if len(filtered_results) < min_results:
            logger.info(
                f"Insufficient filtered results ({len(filtered_results)}). Performing new search."
            )
            redis_cache.delete(cache_key)
            unfiltered_results = get_search_results(media, config)
            redis_cache.set(cache_key, unfiltered_results)
            filtered_results = filter_series_results(unfiltered_results, media)

        logger.info(f"Final number of filtered results: {len(filtered_results)}")
        return filtered_results

    search_results = get_and_filter_results(media, config)

    def stream_processing(search_results, media, config):
        torrent_smart_container = TorrentSmartContainer(search_results, media)

        if config["debrid"]:
            hashes = torrent_smart_container.get_hashes()
            ip = request.client.host
            result = debrid_service.get_availability_bulk(hashes, ip)
            torrent_smart_container.update_availability(
                result, type(debrid_service), media
            )
            logger.info(f"Checked availability for {len(result.items())} items")

        if config["cache"]:
            logger.debug("Caching public container items")
            torrent_smart_container.cache_container_items()

        best_matching_results = torrent_smart_container.get_best_matching()
        best_matching_results = sort_items(best_matching_results, config)
        logger.info(f"Found {len(best_matching_results)} best matching results")

        stream_list = parse_to_stremio_streams(best_matching_results, config, media)
        logger.info(f"Processed {len(stream_list)} streams for Stremio")

        return stream_list

    stream_list = stream_processing(search_results, media, config)
    redis_cache.set(
        cache_key(media), stream_list, expiration=3600
    )  # Make the cache expire after 1 hour TODO: Make this configurable
    total_time = time.time() - start
    logger.info(f"Request completed in {total_time:.2f} seconds")
    return {"streams": stream_list}


@lru_cache(maxsize=128)
def get_adaptive_chunk_size(file_size):
    MB = 1024 * 1024
    GB = 1024 * MB

    if file_size < 1 * GB:
        return 1 * MB  # 1 MB
    elif file_size < 3 * GB:
        return 2 * MB  # 2 MB
    elif file_size < 9 * GB:
        return 5 * MB  # 5 MB
    elif file_size < 20 * GB:
        return 10 * MB  # 10 MB
    else:
        return 20 * MB  # 20 MB


async def proxy_stream(url: str, headers: dict, proxy: str = None):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, proxy=proxy) as response:
            file_size = int(response.headers.get("Content-Length", 0))
            chunk_size = get_adaptive_chunk_size(file_size)

            while True:
                chunk = await response.content.read(chunk_size)
                if not chunk:
                    break
                yield chunk


def get_stream_link(
    decoded_query: str, config: dict, ip: str, redis_cache: RedisCache
) -> str:
    cache_key = f"stream_link:{decoded_query}_{ip}"

    cached_link = redis_cache.get(cache_key)
    if cached_link:
        logger.info(f"Stream link cached: {cached_link}")
        return cached_link

    debrid_service = get_debrid_service(config)
    link = debrid_service.get_stream_link(decoded_query, config, ip)

    redis_cache.set(cache_key, link, expiration=3600)  # Cache for 1 hour
    logger.info(f"Stream link generated and cached: {link}")

    return link


@app.get("/playback/{config}/{query}")
async def get_playback(
    config: str,
    query: str,
    request: Request,
    redis_cache: RedisCache = Depends(get_redis_cache),
):
    try:
        if not query:
            raise HTTPException(status_code=400, detail="Query required.")

        config = parse_config(config)
        decoded_query = decodeb64(query)
        ip = request.client.host

        link = get_stream_link(decoded_query, config, ip, redis_cache)

        range_header = request.headers.get("Range")
        headers = {}
        if range_header:
            headers["Range"] = range_header

        proxy = None  # Not yet implemented

        async with aiohttp.ClientSession() as session:
            async with session.get(link, headers=headers, proxy=proxy) as response:
                if response.status == 206:
                    return StreamingResponse(
                        proxy_stream(link, headers, proxy),
                        status_code=206,
                        headers={
                            "Content-Range": response.headers["Content-Range"],
                            "Content-Length": response.headers["Content-Length"],
                            "Accept-Ranges": "bytes",
                            "Content-Type": "video/mp4",
                        },
                    )
                elif response.status == 200:
                    return StreamingResponse(
                        proxy_stream(link, headers, proxy),
                        headers={
                            "Content-Type": "video/mp4",
                            "Accept-Ranges": "bytes",
                        },
                    )
                else:
                    return RedirectResponse(link, status_code=302)

    except Exception as e:
        logger.error(f"Playback error: {e}")
        raise HTTPException(
            status_code=500, detail="An error occurred while processing the request."
        )


@app.head("/playback/{config}/{query}")
async def head_playback(
    config: str,
    query: str,
    request: Request,
    redis_cache: RedisCache = Depends(get_redis_cache),
):
    try:
        if not query:
            raise HTTPException(status_code=400, detail="Query required.")

        config = parse_config(config)
        decoded_query = decodeb64(query)
        ip = request.client.host

        cache_key = f"stream_link:{decoded_query}_{ip}"

        if redis_cache.exists(cache_key):
            return Response(status_code=status.HTTP_200_OK)
        else:
            await asyncio.sleep(0.2)
            return Response(status_code=status.HTTP_200_OK)
    except Exception as e:
        logger.error(f"HEAD request error: {e}")
        return Response(
            status=500, detail="An error occurred while processing the request."
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
