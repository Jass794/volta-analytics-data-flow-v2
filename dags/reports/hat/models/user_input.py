from dataclasses import dataclass, field

@dataclass
class UserInput:
    report_date: str = None
    environment: str = field(default="", metadata={"validate": lambda value: value in {"staging", "production"}})
    report_type: str = field(default="", metadata={"validate": lambda value: value in {"Voltage", "Current"}})
    debug: bool = False