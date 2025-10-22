import os
import uuid
import datetime as dt
from typing import Optional
from dataclasses import dataclass, field

from fred.utils.dateops import datetime_utcnow

from fred.ogd.layer.catalog import LayerCatalog
from fred.ogd.banxico.timeseries.catalog import BanxicoTimeSeriesCatalog
from fred.ogd.banxico.settings import (
    FRDOGD_SOURCE_FULLNAME,
    FRDOGD_BACKEND_SERVICE,
)


@dataclass(frozen=True, slots=True)
class LayerHelper:
    exec_ts: dt.datetime = field(default_factory=datetime_utcnow)
    exec_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @classmethod
    def auto(cls, **kwargs) -> "LayerHelper":
        return cls(**kwargs)

    def run(self, layer: LayerCatalog | str, backend: Optional[str] = None, **kwargs) -> str:
        match (target_layer := LayerCatalog[layer] if isinstance(layer, str) else layer):
            case LayerCatalog.LANDING:
                # Must-have arguments for landing layer
                timeserie = kwargs.pop("timeserie")
                # Execute landing layer
                return self.landing(
                    layer=target_layer,
                    timeserie=timeserie,
                    backend=backend,
                    **kwargs,
                )
            case _:
                raise NotImplementedError(f"Layer {layer} not implemented yet.")

    def landing(
            self,
            timeserie: str | BanxicoTimeSeriesCatalog,
            layer: Optional[LayerCatalog] = None,
            backend: Optional[str] = None,
            **kwargs
        ) -> str:
        from fred.ogd.source.catalog import SourceCatalog

        layer = layer or LayerCatalog.LANDING
        series = BanxicoTimeSeriesCatalog[timeserie] \
            if isinstance(timeserie, str) else timeserie
        layer_instance = layer.auto(
            source=SourceCatalog.REQUEST.name,
            backend=backend or FRDOGD_BACKEND_SERVICE,  # e.g., MINIO
            source_kwargs={
                "target_url": series.value.url,
                **kwargs.pop("source_kwargs", {}),
            },
            backend_kwargs={
                **kwargs.pop("backend_kwargs", {}),
            },
        )
        return layer_instance.run(
            output_path=os.path.join(
                FRDOGD_SOURCE_FULLNAME,
                layer.name.lower(),
                series.name,
                self.exec_ts.strftime("%Y"),
                self.exec_ts.strftime("%m"),
            ),
            **kwargs,
        )
