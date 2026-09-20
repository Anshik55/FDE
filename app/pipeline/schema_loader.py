"""Schema loader and validator for target entity definitions."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
import yaml


@dataclass
class FieldDef:
    name: str
    type: str  # string, email, phone, date, enum, decimal
    required: bool = False
    sensitive: bool = False
    identity: bool = False
    pattern: Optional[str] = None
    references: Optional[str] = None
    min: Optional[float] = None
    max: Optional[float] = None
    description: str = ""
    values: Dict[str, List[str]] = field(default_factory=dict)  # canonical -> [synonyms]


@dataclass
class TargetSchema:
    entity: str
    fields: Dict[str, FieldDef]

    @classmethod
    def from_yaml(cls, path: str | Path) -> "TargetSchema":
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if not data or "entity" not in data or "fields" not in data:
            raise ValueError("Target schema must define 'entity' and 'fields'.")

        fields: Dict[str, FieldDef] = {}
        for fname, fprops in data["fields"].items():
            fields[fname] = FieldDef(
                name=fname,
                type=fprops.get("type", "string"),
                required=fprops.get("required", False),
                sensitive=fprops.get("sensitive", False),
                identity=fprops.get("identity", False),
                pattern=fprops.get("pattern"),
                references=fprops.get("references"),
                min=fprops.get("min"),
                max=fprops.get("max"),
                description=fprops.get("description", ""),
                values=fprops.get("values", {}) or {},
            )

        return cls(entity=data["entity"], fields=fields)

    def get_identity_fields(self) -> List[FieldDef]:
        return [f for f in self.fields.values() if f.identity]

    def get_required_fields(self) -> List[FieldDef]:
        return [f for f in self.fields.values() if f.required]

    def get_sensitive_fields(self) -> List[FieldDef]:
        return [f for f in self.fields.values() if f.sensitive]
