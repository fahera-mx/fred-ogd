from dataclasses import dataclass

from fred.worker.interface import HandlerInterface
from fred.utils.dateops import datetime_utcnow


@dataclass(frozen=True, slots=False)
class LayerHandler(HandlerInterface):

    def handler(self, payload: dict) -> dict:
        from fred.ogd.banxico.helper import LayerHelper

        started_at = datetime_utcnow()
        layer_helper = LayerHelper.auto()
        layer = payload.pop("layer")
        ok = layer_helper.run(
            layer=layer,
            timeserie=(timeserie := payload.pop("timeserie")),
            backend=payload.pop("backend", None),
            **payload,
        )
        return {
            "layer": layer,
            "layer_exec_ok": ok,
            "layer_exec_id": layer_helper.exec_id,
            "layer_exec_ts": layer_helper.exec_ts.isoformat(),
            "handler_started_at": started_at.isoformat(),
            "handler_ended_at": datetime_utcnow().isoformat(),
            "timeserie": timeserie,
        }
