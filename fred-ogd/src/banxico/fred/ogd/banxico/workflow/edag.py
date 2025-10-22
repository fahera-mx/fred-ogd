from fred.edag import node
from fred.edag.plan import Plan
from fred.ogd.workflow import WorkflowInterface
from fred.ogd.banxico.timeseries.catalog import (
    BanxicoTimeSeriesCatalog,
)


@node(inplace=True)
def timeseries(group: str) -> list[BanxicoTimeSeriesCatalog]:
    return BanxicoTimeSeriesCatalog.group(name=group)

@node(inplace=True)
def layer_landing(timeserie: BanxicoTimeSeriesCatalog) -> BanxicoTimeSeriesCatalog:
    from fred.ogd.banxico.helper import LayerHelper
    from fred.ogd.layer.catalog import LayerCatalog

    layer_helper = LayerHelper.auto()
    layer_helper.run(
        layer=LayerCatalog.LANDING,
        timeserie=timeserie,
    )

    return timeserie

@node(inplace=True)
def layer_bronze(timeserie: BanxicoTimeSeriesCatalog) -> str:
    return timeserie.name


@node(inplace=True)
def layer_silver(timeseries: list[str]) -> str:
    return ", ".join(timeseries)

@node(inplace=True)
def layer_gold(silver_table: str) -> str:
    return silver_table


class Workflow(WorkflowInterface):

    @property
    def name(self) -> str:
        from fred.ogd.banxico.settings import FRDOGD_SOURCE_FULLNAME
        return FRDOGD_SOURCE_FULLNAME

    @property
    def plan(self) -> Plan:
        return (
            timeseries[...]
                >> layer_landing[...]
                    >> layer_bronze.with_output(key="timeseries")
                        >> layer_silver.with_output(key="silver_table")
                            >> layer_gold.with_output(key="gold_table")
        )
