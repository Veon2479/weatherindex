import json
import os
import typing
import pandas as pd
from dateutil.parser import isoparse
from metrics.parse.base_parser import BaseParser

from rich.console import Console
from metrics.utils.precipitation import PrecipitationType

from pprint import pprint


console = Console()


def _parse_time(time_str: str) -> int:
    return int(isoparse(time_str).timestamp())


def _parse_precip_type(precip_type: str) -> PrecipitationType:
    if precip_type == "rain":
        return PrecipitationType.RAIN
    elif precip_type == "snow":
        return PrecipitationType.SNOW
    elif precip_type == "precip":
        return PrecipitationType.MIX

    raise ValueError(f"Unknown precipitation type: {precip_type}")


def interp(x1, x2, y1, y2, x):
    k = (y1 - y2) / (x1 - x2)
    b = y1 - k * x1
    return k * x + b


class WeatherCompanyParser(BaseParser):

    INTERPOLATE = True
    DYNAMIC_OFFSET = True
    INTERP_STEP = 5 * 60

    def _parse_impl(self, timestamp: int, file_name: str, data: bytes) -> typing.List[typing.List[any]]:
        """See :func:`~metrics.base_parser.BaseParser._parse_impl`"""
        rows = []
        data_json = json.loads(data)
        sensor_id = os.path.basename(file_name).replace(".json", "")

        lat = data_json["position"]["lat"]
        lon = data_json["position"]["lon"]

        local_times_str_list = data_json["payload"]["validTimeLocal"]
        precip_types = data_json["payload"]["precipType"]
        precip_rates = data_json["payload"]["precipRate"]
        snow_rates = data_json["payload"]["snowRate"]
        precip_probs = data_json["payload"]["precipChance"]

        if self.DYNAMIC_OFFSET:
            timestamp = min(_parse_time(t) for t in local_times_str_list)

        for local_time_str, precip_type, precip_rate, snow_rate, precip_prob in zip(local_times_str_list,
                                                                                    precip_types,
                                                                                    precip_rates,
                                                                                    snow_rates,
                                                                                    precip_probs):
            forecast_timestamp = int(_parse_time(local_time_str))
            precip_type = _parse_precip_type(precip_type)
            snow_rate = snow_rate * 10.0
            precip_rate = precip_rate
            precip_prob = precip_prob / 100.0

            if precip_prob == 0.0:
                precip_type = PrecipitationType.UNKNOWN

            if precip_type == PrecipitationType.MIX:
                precip_rate = max(precip_rate, snow_rate)
            elif precip_type == PrecipitationType.SNOW:
                precip_rate = snow_rate

            rows.append((sensor_id,
                         lon, lat,
                         forecast_timestamp, forecast_timestamp - timestamp,
                         precip_rate,
                         precip_prob,
                         precip_type))

        if not self.INTERPOLATE:
            return rows
        else:

            # orig = pd.DataFrame(rows, columns=self._get_columns())  # for later debug only

            last_row = rows.pop(0)
            interpolated = [last_row]

            for row in rows:

                for target_ts in range(last_row[3] + self.INTERP_STEP, row[3], self.INTERP_STEP):
                    interpolated_forecast = int(interp(last_row[3],
                                                       row[3],
                                                       last_row[3],
                                                       row[3],
                                                       target_ts))
                    interpolated.append((*row[:3],  # sid, lon, lat
                                         interpolated_forecast,  # forecast timestamp
                                         interpolated_forecast - timestamp,  # forecast offset
                                         interp(last_row[3],
                                                row[3],
                                                last_row[5],
                                                row[5],
                                                target_ts),  # precip rate
                                         interp(last_row[3],
                                                row[3],
                                                last_row[6],
                                                row[6],
                                                target_ts),  # precip prob
                                         # precip type - just use nearest
                                         last_row[7] if target_ts < last_row[3] + (last_row[3] - row[3]) / 2 else row[7]))

                last_row = row
                interpolated.append(row)

            # console.log(f"Original: {orig}, \nInterpolated: {pd.DataFrame(interpolated, columns=self._get_columns())}")

            return interpolated

    def _should_parse_file_extension(self, file_extension: str) -> bool:
        """See :func:`~metrics.base_parser.BaseParser._should_parse_file_extension`"""
        return file_extension == ".json"

    def _get_columns(self) -> typing.List[str]:
        """See :func:`~metrics.base_parser.BaseParser._get_columns`"""
        return ["id", "lon", "lat", "timestamp", "forecast_offset", "precip_rate", "precip_prob", "precip_type"]
