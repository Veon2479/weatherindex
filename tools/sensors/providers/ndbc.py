import aiohttp
import io
import logging
import os
import re
import tarfile

from collections import Counter
from datetime import datetime
from tools.sensors.utils.http import Http
from tools.sensors.providers.provider import BaseProvider
from typing_extensions import override


DAY_SECONDS = 24 * 60 * 60


class NdbcProvider(BaseProvider):

    def __init__(self, frequency: int = DAY_SECONDS, delay: int = 5, **kwargs):
        super().__init__("NDBC", frequency, delay, **kwargs)

    @override
    async def fetch_job(self, timestamp: int):
        """
        Fetch the data for the given timestamp

        Parameters
        ----------
        timestamp : int
            The timestamp of the data to fetch
        """
        data = None

        async with aiohttp.ClientSession() as session:
            logging.info("Fetching sensor id list")
            raw = await Http.get_with_session("https://tao.ndbc.noaa.gov/tao/data_download/tao-esri.html", session)

            if raw is None:
                logging.error("Failed to fetch sensor id list")
                return

            logging.info("Parsing sensors id list")
            html = raw.decode("utf-8")
            sensor_pattern = r"\s{2,}addStation\(-?\d+,\s*-?\d+,\s*'\d+',\s*'(?P<id>[A-Z0-9]+)',\s*'(?P<group>TAO|TRITON)'\)"
            parsed_sensors = re.findall(sensor_pattern, html)
            if len(parsed_sensors) == 0:
                logging.error("No parseable sensor was found")
                return

            station_ids, station_groups = zip(*parsed_sensors)

            group_index = dict(Counter(station_groups))
            logging.info(f"Discovered {len(group_index)} sensor groups:")
            for k, v in group_index.items():
                logging.info(f"Group `{k}`: {v} sensors")

            logging.info("Requesting sensors observation data")
            ts1 = datetime.utcfromtimestamp(timestamp - self._frequency)
            ts2 = datetime.utcfromtimestamp(timestamp)

            url = ("https://tao.ndbc.noaa.gov/tao/data_download/process_results.php?type=rain&reso=hres"
                   f"&year1={ts1.year}&mon1={ts1.month:02d}&day1={ts1.day:02d}"
                   f"&year2={ts2.year}&mon2={ts2.month:02d}&day2={ts2.day:02d}"
                   "&format=subcdf&compression=gzip&"
                   f"stations={'.'.join(station_ids)}")

            raw = await Http.get_with_session(url, session)
            if raw is None:
                logging.error(f"Failed to query observation data from {url}")
                return

            logging.info("Discovering download link for observation data")
            html = raw.decode("utf-8")
            match = re.search(r"<a href=\"(?P<link>cache/\d{6}/[a-zA-Z0-9\-]+\.tar\.gz)\">compressed file</a>", html)
            if match is None:
                logging.error(f"Failed to discover download link from {url}")
                logging.info(html)
                return

            logging.info("Downloading observation data")
            url = os.path.join("https://tao.ndbc.noaa.gov/tao/data_download", match.group("link"))
            data = await Http.get_with_session(url, session)

        if data is None:
            logging.error(f"Failed to download observations")
            return

        logging.info(f"Downloaded {len(data)} bytes of observation data")

        logging.info("Saving observation data")
        file_like = io.BytesIO(data)
        with tarfile.open(fileobj=file_like, mode="r:gz") as tar:
            for member in tar.getmembers():
                f = tar.extractfile(member)
                if f is not None:
                    logging.info(f"Extracted {member.name}: {member.size} bytes")
                    await self._store_file(file_path=member.name,
                                           file_content=f.read())
