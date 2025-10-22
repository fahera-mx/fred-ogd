import enum

import requests

from fred.ogd.banxico.timeseries.interface import BanxicoTimeSeriesInterface
from fred.ogd.banxico.timeseries._usd_mxn import BanxicoTimeSeriesUsdMxn


class BanxicoTimeSeriesGroup(enum.Enum):
    USD_MXN = BanxicoTimeSeriesUsdMxn

    @property
    def classref(self) -> type[BanxicoTimeSeriesInterface]:
        return self.value


class BanxicoTimeSeriesCatalog(enum.Enum):
    USD_MXN = BanxicoTimeSeriesUsdMxn.from_config(serie="USD_MXN")
    USD_MXN_DOS = BanxicoTimeSeriesUsdMxn.from_config(serie="USD_MXN_DOS")
    USD_MXN_DOD = BanxicoTimeSeriesUsdMxn.from_config(serie="USD_MXN_DOD")
    USD_MXN_48OPENB = BanxicoTimeSeriesUsdMxn.from_config(serie="USD_MXN_48OPENB")
    USD_MXN_48OPENS = BanxicoTimeSeriesUsdMxn.from_config(serie="USD_MXN_48OPENS")
    USD_MXN_48CLOSEB = BanxicoTimeSeriesUsdMxn.from_config(serie="USD_MXN_48CLOSEB")
    USD_MXN_48CLOSES = BanxicoTimeSeriesUsdMxn.from_config(serie="USD_MXN_48CLOSES")
    USD_MXN_48MAX = BanxicoTimeSeriesUsdMxn.from_config(serie="USD_MXN_48MAX")
    USD_MXN_48MIN = BanxicoTimeSeriesUsdMxn.from_config(serie="USD_MXN_48MIN")

    @property
    def serie(self) -> BanxicoTimeSeriesInterface:
        return self.value

    def classgroup(self, ref: type[BanxicoTimeSeriesInterface]) -> bool:
        return issubclass(ref, BanxicoTimeSeriesInterface)

    @classmethod
    def group(cls, name: str | BanxicoTimeSeriesGroup) -> list["BanxicoTimeSeriesCatalog"]:
        classref = BanxicoTimeSeriesGroup[name].classref \
            if isinstance(name, str) else name.classref
        return [item for item in cls if item.classgroup(classref)]

    @property
    def request(self) -> requests.Response:
        return self.serie.request.resolve()

    def fetch(self, values_only: bool = False) -> dict | list:
        if values_only:
            return self.fetch(values_only=False).get("valores", [])
        out = self.value.fetch()
        return out.resolve()
