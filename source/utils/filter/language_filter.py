from utils.filter.base_filter import BaseFilter
from utils.logger import setup_logger

logger = setup_logger(__name__)


class LanguageFilter(BaseFilter):
    def __init__(self, config):
        super().__init__(config)

    def filter(self, data):
        filtered_data = []
        for torrent in data:
            if not torrent.languages:
                continue

            if "multi" in torrent.languages or any(lang in self.config['languages'] for lang in torrent.languages):
                filtered_data.append(torrent)

        return filtered_data

    def can_filter(self):
        return self.config['languages'] is not None
