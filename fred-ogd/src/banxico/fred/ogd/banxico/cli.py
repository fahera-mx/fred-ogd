import os
from typing import Optional

from fred.cli.interface import AbstractCLI

from fred.ogd.layer.catalog import LayerCatalog
from fred.ogd.banxico.helper import LayerHelper
from fred.ogd.banxico.timeseries.catalog import BanxicoTimeSeriesCatalog


class OGDExtCLI(AbstractCLI):

    def timeseries(self) -> dict[str, str]:
        return {
            item.name: item.value.description or item.value.reference or item.value.code
            for item in BanxicoTimeSeriesCatalog
        }

    def layer_exec(self, layer: str, backend: Optional[str] = None, **kwargs) -> str:
        layer_helper = LayerHelper.auto()
        return layer_helper.run(
            layer=LayerCatalog[layer],
            backend=backend,
            **kwargs,
        )

    def version(self) -> str:
        version_filepath = os.path.join(
            os.path.dirname(__file__),
            "version"
        )
        with open(version_filepath, "r", encoding="utf-8") as version_file:
            return version_file.read().strip()

    def workflow(self, **kwargs):
        from fred.ogd.banxico.workflow.edag import Workflow
        # Load workflow from config file
        workflow = Workflow.from_file(
            version=self.version(),
            filepath=os.path.join(
                os.path.dirname(__file__),
                "workflow",
                "config.json",
            ),
        )
        # Run workflow
        return workflow.run(**kwargs)
