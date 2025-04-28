from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve


class Testnoaa:
    def test_noaa(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "Testnoaa",
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
                print(retriever)
                # dataset.update_from_yaml(
                #     path=join(config_dir, "hdx_dataset_static.yaml")
                # )
