from dataclasses import dataclass, field

@dataclass
class HatConfigsModel:
    lt_data_days: int

    # Configs for the scan scan duration
    st_scan_look_back_days: int
    st_scan_lt_avg_days: int 
    st_scan_st_avg_days: int

    lt_scan_look_back_days: int 
    lt_scan_lt_avg_days: int
    lt_scan_st_avg_days: int
