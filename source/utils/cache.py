import copy
import os
from typing import List
import redis
import pickle

from jackett.jackett_result import JackettResult
from utils.logger import setup_logger

logger = setup_logger(__name__)

# Redis connection initialization
redis_client = redis.Redis(host="redis", port=6379)

CACHE_EXPIRATION_TIME = 48 * 60 * 60

def search_cache(media):
    try:
        logger.info(f"Searching for cached {media.type} results")
        cache_key = f"{media.type}:{media.titles[0]}:{media.languages[0]}"
        cached_result = redis_client.get(cache_key)
        return pickle.loads(cached_result) if cached_result else []
    except Exception as e:
        logger.error(f"Error in search_cache: {str(e)}")
        return []

def cache_results(torrents: List[JackettResult], media):
    if os.getenv("NODE_ENV") == "development":
        return

    logger.info("Started caching results")

    cached_torrents = []
    for torrent in torrents:
        try:
            # Créer une copie profonde de l'objet torrent
            cached_torrent = copy.deepcopy(torrent)
            cached_torrent.indexer = "Locally cached"
            cached_torrents.append(cached_torrent)
        except Exception as e:
            logger.error(f"Failed to create cached copy of torrent: {str(e)}")

    try:
        cache_key = f"{media.type}:{media.titles[0]}:{media.languages[0]}"
        pickled_data = pickle.dumps(cached_torrents)
        redis_client.set(cache_key, pickled_data)
        redis_client.expire(cache_key, CACHE_EXPIRATION_TIME)

        logger.info(f"Cached {len(cached_torrents)} {media.type} results")
    except Exception as e:
        logger.error(f"Failed to cache results: {str(e)}")