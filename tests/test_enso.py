from os.path import join

import pandas as pd
import pytest
from hdx.api.configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from hdx.scraper.enso.enso import ENSO


class TestENSO:
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
        return join("src", "hdx", "scraper", "enso", "config")

    def test_enso(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestENSO",
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

                enso = ENSO(configuration, retriever, tempdir)

                enso_data = enso.process_enso()
                first_row = enso_data.iloc[0].to_dict()
                assert first_row == {
                    "date": pd.Timestamp("1950-01-01 00:00:00"),
                    "ANOM": -1.62,
                    "ANOM_trimester": -1.3366666666666667,
                    "ANOM_trimester_round": -1.3,
                    "ClimAdjust": 26.18,
                    "TOTAL": 24.56,
                    "phase_event": "lanina",
                    "phase_trimester": "lanina",
                }

                dataset = enso.generate_dataset()
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )

                assert dataset == {
                    "name": "el-nino-southern-oscillation-enso-el-nino-and-la-nina-events",
                    "title": "El Niño-Southern Oscillation (ENSO): El Niño and La Niña Events",
                    "dataset_date": "[1950-01-01T00:00:00 TO 2025-10-01T23:59:59]",
                    "tags": [
                        {
                            "name": "climate hazards",
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
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "license_id": "cc-by",
                    "methodology": "[ENSO FAQ](https://www.cpc.ncep.enso.gov/products/analysis_monitoring/ensostuff/ensofaq.shtml#ENSO)  \n"
                    " [Description of Changes to Ocean Niño Index (ONI)](https://www.cpc.ncep.enso.gov/products/analysis_monitoring/ensostuff/ONI_change.shtml)",
                    "caveats": "None",
                    "dataset_source": "National Oceanic and Atmospheric Administration (enso) / Climate Prediction Center (CPC)",
                    "groups": [{"name": "world"}],
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "b682f6f7-cd7e-4bd4-8aa7-f74138dc6313",
                    "owner_org": "hdx",
                    "data_update_frequency": 30,
                    "notes": "ENSO stands for El Niño/ Southern Oscillation. The ENSO cycle refers to the coherent and sometimes very strong year-to-year variations in sea- surface temperatures, convective rainfall, surface air pressure, and atmospheric circulation that occur across the equatorial Pacific Ocean. El Niño and La Niña represent opposite extremes in the ENSO cycle.  \n"
                    " El Niño refers to the above-average sea-surface temperatures that periodically develop across the east-central equatorial Pacific. It represents the warm phase of the ENSO cycle, and is sometimes referred to as a Pacific warm episode.  \n"
                    " La Niña refers to the periodic cooling of sea-surface temperatures across the east-central equatorial Pacific. It represents the cold phase of the ENSO cycle, and is sometimes referred to as a Pacific cold episode.  \n"
                    " \n [View the raw data](https://www.cpc.ncep.enso.gov/products/analysis_monitoring/ensostuff/detrend.nino34.ascii.txt)  \n"
                    " [View the processed data](https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/ensostuff/ONI_v5.php)  \n"
                    " \n Dataset field definitions:  \n"
                    " date: Year-month-date (first day of month)  \n"
                    " TOTAL: ERSST.v5 (sea surface temperature) in the Niño 3.4 region  \n"
                    " ClimAdjust: adjustment for changing 30-year base period  \n"
                    " ANOM: Oceanic Niño Index (ONI)  \n"
                    " ANOM_trimester: 3-month rolling mean of ANOM (MO corresponds to first month of trimester)  \n"
                    " ANOM_trimester_round: ANOM_trimester rounded to one decimal place  \n"
                    " phase_trimester: phase of trimester based on ANOM_trimester_round  \n"
                    " phase_event: elnino or lanina event (requiring at least 5 consecutive trimesters of the phase)",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "el-nino-southern-oscillation-enso-el-nino-and-la-nina-events.csv",
                        "description": "Monthly analysis of the El Niño-Southern Oscillation (ENSO) "
                        "cycle",
                        "format": "csv",
                    }
                ]
