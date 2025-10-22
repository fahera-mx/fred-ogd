import textwrap
from dataclasses import dataclass
from typing import Callable

from fred.edag.plan import Plan
from fred.ogd.version import version


class WorkerAbstractInterface:
    plan: Plan


@dataclass(frozen=True, slots=True)
class WorkflowInterface(WorkerAbstractInterface):
    name: str
    version: str
    specs: list[dict]

    @classmethod
    def from_config(cls, version: str, config: dict) -> "WorkflowInterface":
        return cls(
            name=config["name"],
            specs=config.get("specs", []),
            version=version,
        )
    @classmethod
    def from_file(cls, version: str, filepath: str) -> "WorkflowInterface":
        import json
        with open(filepath, "r") as file:
            config = json.load(file)
        return cls.from_config(
            version=version,
            config=config,
        )
    def get_edags(self, **kwargs) -> dict[str, Callable]:
        from fred.edag.executor import Executor
        return {
            spec["name"]: lambda **params:
                Executor.from_plan(plan=self.plan).execute(
                    **{
                        "start_with": spec.get("parameters", {}),
                        **kwargs,
                        **params
                    }
                )
            for spec in self.specs
        }

    def run(self, **kwargs):
        out = {}
        edags = self.get_edags(**kwargs)
        for name, edag in edags.items():
            edag_out = edag()
            out[name] = {
                key: edag_out["results"][key]
                for key in edag_out["layers"][-1]
            }
        return out
