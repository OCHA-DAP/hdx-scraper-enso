#!/usr/bin/python
"""enso scraper"""

import logging
from io import StringIO
from typing import Optional

import pandas as pd
import requests
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)


class ENSO:
    def __init__(
        self, configuration: Configuration, retriever: Retrieve, temp_dir: str
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir

    def process_enso(self) -> pd.DataFrame:
        base_url = self._configuration["base_url"]
        response = requests.get(base_url)
        response.raise_for_status()

        data = StringIO(response.text)

        df = pd.read_csv(data, sep=r"\s+")

        def anom_to_phase(anom):
            if anom >= 0.5:
                return "elnino"
            elif anom <= -0.5:
                return "lanina"
            else:
                return "neutral"

        def label_event_phase(group, phase_name):
            # Identify sequences with at least 5 consecutive identical phase names
            count = 0
            for i in range(len(group)):
                if group[i] == phase_name:
                    count += 1
                    if count >= 5:
                        df.loc[i - count + 1 : i, "phase_event"] = phase_name
                else:
                    count = 0

        df["date"] = df["YR"].astype(str) + "-" + df["MON"].astype(str) + "-01"
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        # Remove year and month columns now that we have date
        df.drop(["YR", "MON"], axis=1, inplace=True)

        # Reorder columns so date is first
        cols = ["date"] + [c for c in df.columns if c != "date"]
        df = df[cols]

        df["ANOM_trimester"] = df["ANOM"].rolling(window=3).mean().shift(-2)
        df["ANOM_trimester_round"] = df["ANOM_trimester"].round(1)
        df["phase_trimester"] = df["ANOM_trimester_round"].apply(anom_to_phase)
        df["phase_event"] = "neutral"

        label_event_phase(df["phase_trimester"], "elnino")
        label_event_phase(df["phase_trimester"], "lanina")

        return df

    def generate_dataset(self) -> Optional[Dataset]:
        df = self.process_enso()

        # Create dataset
        dataset_info = self._configuration
        dataset_title = dataset_info["title"]
        slugified_name = slugify(f"{dataset_info['title']}")

        dataset = Dataset({"name": slugified_name, "title": dataset_title})

        # Add dataset info
        dataset.add_other_location("world")
        dataset.add_tags(dataset_info["tags"])

        start_date = df["date"].min()
        end_date = df["date"].max()
        dataset.set_time_period(start_date, end_date)

        # Create resource
        resource_name = f"{slugified_name}.csv"
        resource_description = dataset_info["description"]
        resource = {
            "name": resource_name,
            "description": resource_description,
        }

        dataset.generate_resource_from_iterable(
            headers=df.columns.tolist(),
            iterable=df.to_dict(orient="records"),
            hxltags=dataset_info["hxl_tags"],
            folder=self._temp_dir,
            filename=resource_name,
            resourcedata=resource,
            quickcharts=None,
        )

        return dataset
