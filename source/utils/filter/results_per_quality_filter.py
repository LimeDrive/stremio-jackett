from utils.filter.base_filter import BaseFilter
from utils.logger import setup_logger

logger = setup_logger(__name__)


class ResultsPerQualityFilter(BaseFilter):
    def __init__(self, config):
        super().__init__(config)
        self.max_results_per_quality = int(self.config.get('resultsPerQuality', 5))

    def filter(self, data):
        filtered_items = []
        resolution_count = {}

        for item in data:
            resolutions = getattr(item.parsed_data, 'resolution', [])
            if not resolutions:
                resolutions = ["?.BZH.?"]

            logger.info(f"Filtering by quality: {resolutions}")
            
            for resolution in resolutions:
                if resolution not in resolution_count:
                    resolution_count[resolution] = 1
                    filtered_items.append(item)
                    break
                elif resolution_count[resolution] < self.max_results_per_quality:
                    resolution_count[resolution] += 1
                    filtered_items.append(item)
                    break

        logger.info(f"ResultsPerQualityFilter: input {len(data)}, output {len(filtered_items)}")
        return filtered_items

    def can_filter(self):
        return self.max_results_per_quality > 0
