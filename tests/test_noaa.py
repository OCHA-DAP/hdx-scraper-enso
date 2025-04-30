from os.path import join

import pandas as pd
import pytest
from hdx.api.configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from hdx.scraper.noaa.noaa import NOAA


class TestNOAA:
    @pytest.fixture(scope="function")
    def configuration(self, config_dir):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join(config_dir, "project_configuration.yaml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture(scope="class")
    def config_dir(self, fixtures_dir):
        return join("src", "hdx", "scraper", "noaa", "config")

    def test_noaa(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestNOAA",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )

                noaa = NOAA(configuration, retriever, tempdir)

                noaa_data = noaa.process_enso()
                first_row = noaa_data.iloc[0].to_dict()
                assert first_row == {
                    "ANOM": -1.62,
                    "ANOM_trimester": -1.3366666666666667,
                    "ANOM_trimester_round": -1.3,
                    "ClimAdjust": 26.18,
                    "MON": 1,
                    "TOTAL": 24.56,
                    "YR": 1950,
                    "date": pd.Timestamp("1950-01-01 00:00:00"),
                    "phase_longterm": "lanina",
                    "phase_trimester": "lanina",
                }

                dataset = noaa.generate_dataset()
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )

                assert dataset == {
                    "name": "enso-el-nino-southern-oscillation",
                    "title": "ENSO: El Nino-Southern Oscillation",
                    "dataset_date": "[1950-01-01T00:00:00 TO 2025-03-01T23:59:59]",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "climate-weather",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "el nino-el nina",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "license_id": "cc-by",
                    "methodology": "https://www.cpc.ncep.noaa.gov/products/precip/CWlink/MJO/enso.shtml",
                    "caveats": "None",
                    "dataset_source": "National Oceanic and Atmospheric Administration (NOAA) / Climate Prediction Center (CPC)",
                    "groups": [{"name": "world"}],
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "2f9fd160-2a16-49c0-89d6-0bc3230599bf",
                    "owner_org": "hdx",
                    "data_update_frequency": 30,
                    "notes": "The Climate Prediction Center's (CPC) products are operational predictions "
                    "of climate variability, real-time monitoring of climate and the required data bases, "
                    "and assessments of the origins of major climate anomalies. The products cover time "
                    "scales from a week to seasons, extending into the future as far as technically feasible, "
                    "and cover the land, the ocean, and the atmosphere, extending into the stratosphere.",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "enso-el-nino-southern-oscillation.csv",
                        "description": "csv file",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    }
                ]
