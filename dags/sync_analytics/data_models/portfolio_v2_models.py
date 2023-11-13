from typing import Optional, List, Any, Tuple

from pydantic import BaseModel, validator
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

from pydantic import BaseModel


class FaultLibraryAlertFlags(BaseModel):
    current_thd_alert: Optional[bool] = False
    undervoltage_alert: Optional[bool] = False
    distribution_ground_fault_alert: Optional[bool] = False
    # motor_failure: Optional[bool] = False
    overvoltage_alert: Optional[bool] = False
    sel_alarms_trip_count_alert: Optional[bool] = False
    v_over_i_change: Optional[bool] = False
    high_event_count_alert: Optional[bool] = False
    overcurrent_alert: Optional[bool] = False
    multiple_start_counts_fault: Optional[bool] = False
    sel_max_voltage_imbalance_alert: Optional[bool] = False
    i_1_ph: Optional[bool] = False
    nema_start_limit_cross_alert: Optional[bool] = False
    current_imbalance_alert: Optional[bool] = False
    load_imbalance_alert: Optional[bool] = False
    power_drop_fault: Optional[bool] = False
    sel_voltage_imbalance_alert: Optional[bool] = False
    voltage_thd_alert: Optional[bool] = False
    sel_current_imbalance_alert: Optional[bool] = False
    sel_overcurrent_alert: Optional[bool] = False
    loss_of_phase_fault: Optional[bool] = False
    sel_max_current_imbalance_alert: Optional[bool] = False
    voltage_imbalance_alert: Optional[bool] = False
    sel_undervoltage_alert: Optional[bool] = False
    sel_overvoltage_alert: Optional[bool] = False
    event_distribution_ground_fault_alert: Optional[bool] = False
    transient_distribution_ground_fault_alert: Optional[bool] = False
    unusual_stop_event_alert: Optional[bool] = False



# Portal response
class NodeDetails(BaseModel):
    activeIa: bool
    activeIb: bool
    activeIc: bool
    activePa: bool
    activePb: bool
    activePc: bool
    activeVa: bool
    activeVb: bool
    activeVc: bool
    container: str
    currentBoundaryUpperPercent: float
    currentDeploymentStatus: str
    currentDeploymentStatusUpdateTimestamp: int
    currentImbalanceBoundaryUpperPercent: float
    currentImbalanceThreshold: float
    currentThdBoundaryUpperPercent: float
    currentThdThreshold: float
    dataPointInterval: int
    deploymentIssue: bool
    deprecationNotice: str
    disableMonitoring: bool
    id: str
    ipAddress: str
    lossOfPhaseTransientThreshold: float
    macAddress: str
    monitorDataPointAlerts: bool
    nameplateValid: bool
    newDeploymentStatus: str
    newDeploymentStatusUpdateTimestamp: int
    notifyOnConnect: bool
    notifyOnCurrentAboveNoise: bool
    overCurrentThreshold: float
    overVoltageThreshold: float
    pauseNotifications: bool
    serialNumber: str
    showRawPower: bool
    type: str
    underVoltageThreshold: float
    version: int
    voltageBoundaryUpperPercent: float
    voltageImbalanceBoundaryUpperPercent: float
    voltageImbalanceThreshold: float
    voltageThdBoundaryUpperPercent: float
    voltageThdThreshold: float


# Analytics
class NodeConfigs(BaseModel):
    i_noise: float = 1
    v_noise: float = 30
    np_voltage: float = 0.1
    np_current: float = 0.1
    np_frequency: float = 0.1
    np_hp: float = 0.1
    np_running_speed: float = 0.1
    np_poles: float = 0.1
    np_rotor_bars: float = 0.1
    np_stator_slots: float = 0.1
    wc: float = 0
    eq_type: str = 'none'
    eq_type_sub: str = 'none'
    tr_i_max: float = 20000.0
    np_sf: float = 1.15
    events_active: bool = False

    @validator('tr_i_max')
    def make_num_division(cls, val):
        # this is implemented because node can't handle floating numbers here we divide it
        if len(str(val)) >= 9:
            float_removed_num = round(val / 10000, 2)
        else:
            float_removed_num = val
        return float_removed_num


# Portal response
class DecommissionedNode(BaseModel):
    facilityName: str
    serialNumber: str
    siteName: str


# Portal response
# class EquipmentStartDate(BaseModel):
#     date: int
#     day: int
#     hours: int
#     minutes: int
#     month: int
#     seconds: int
#     time: int
#     timezoneOffset: int
#     year: int


# Portal response
class Location(BaseModel):
    ctLocation: str
    customerXRefId: str
    displayName: str
    equipmentStartTimestamp: int
    equipmentType: str
    id: str
    loadApplication: str
    locationNodeIds: dict
    name: str
    nodes: List[NodeDetails]
    nodeSerialNumber: str
    notifyMissedData: bool
    npCurrent: Any
    npHp: Any
    npPoles: Any
    npRotorBars: Any
    npRpm: Any
    npVoltage: Any
    secondaryNodeSerialNumber: Any
    starter: str
    timezone: str
    voltageTapLocation: str
    faultLibraryAlertFlags: FaultLibraryAlertFlags


# Portal response
class Facility(BaseModel):
    customerXRefId: str
    division: str
    id: str
    locations: List[Location]
    managers: List
    name: str
    siteName: str
    facilityLocation: Optional[str] = None



# Portal response
class PortalApiModel(BaseModel):
    code: str
    decommissionedNodes: List[DecommissionedNode]
    enableDataTransfer: bool
    facilities: List[Facility]
    id: str
    name: str
    system: bool


class PortfolioV2Model(BaseModel):
    # id: int
    customer_id: str
    customer_code: str
    facility_id: str
    location_id: str
    location_node_id: str
    node_sn: int
    customer_name: str
    facility_name: str
    location_name: str
    equipment_type: str
    location_timezone: str
    data_start_epoch: int
    equipment_start_epoch: int
    work_cycle: bool
    np_voltage: float
    np_current: float
    np_frequency: float
    np_hp: Optional[float] = None
    np_rpm: Optional[float] = None
    np_poles: Optional[float] = None
    np_rotor_bars: Optional[float] = None
    np_stator_slots: Optional[float] = None
    belt_frequency_scan: bool
    belt_frequencies: Optional[list] = None
    gearbox_frequency_scan: bool
    gearbox_frequencies: Optional[list] = None
    current_rise_scan: bool
    trending_report_scan: bool
    current_hat_report_scan: bool
    voltage_hat_report_scan: bool
    events_report_scan: bool
    i_noise: float
    v_noise: float
    starter: Optional[str] = None
    motor_change_epoch: Optional[list] = None
    load_application: Optional[str] = None
    ct_location: Optional[str] = None
    v_tap_location: Optional[str] = None
    load_imbalance_pairs: Optional[list] = None
    harmonic_frequencies_scan: Optional[dict] = None
    tr_i_max: Optional[float] = None
    active_ia: Optional[bool] = None
    active_ib: Optional[bool] = None
    active_ic: Optional[bool] = None
    active_pa: Optional[bool] = None
    active_pb: Optional[bool] = None
    active_pc: Optional[bool] = None
    container: Optional[str] = None
    voltage_boundary_upper_percent: Optional[float] = None
    voltage_imbalance_boundary_upper_percent: Optional[float] = None
    voltage_imbalance_threshold: Optional[float] = None
    voltage_thd_boundary_upper_percent: Optional[float] = None
    voltage_thd_threshold: Optional[float] = None
    current_boundary_upper_percent: Optional[float] = None
    current_deployment_status: Optional[str] = None
    current_deployment_status_update_timestamp: Optional[int] = None
    current_imbalance_boundary_upper_percent: Optional[float] = None
    current_imbalance_threshold: Optional[float] = None
    current_thd_boundary_upper_percent: Optional[float] = None
    current_thd_threshold: Optional[float] = None
    deployment_issue: Optional[bool] = None
    deprecation_notice: Optional[str] = None
    disable_monitoring: Optional[bool] = None
    ip_address: Optional[str] = None
    loss_of_phase_transient_threshold: Optional[float] = None
    mac_address: Optional[str] = None
    monitor_data_point_alerts: Optional[bool] = None
    nameplate_valid: Optional[bool] = None
    new_deployment_status: Optional[str] = None
    new_deployment_status_update_timestamp: Optional[int] = None
    notify_on_connect: Optional[bool] = None
    notify_on_current_above_noise: Optional[bool] = None
    over_current_threshold: Optional[float] = None
    over_voltage_threshold: Optional[float] = None
    pause_notifications: Optional[bool] = None
    product_type: Optional[str] = None
    under_voltage_threshold: Optional[float] = None
    facility_location: Optional[Tuple[float, float]] = None

    class Config:
        orm_mode = True


class PortfolioModelSyncAnalytics(BaseModel):
    customer_id: str
    customer_code: str
    facility_id: str
    location_id: str
    location_node_id: str
    node_sn: int
    secondary_node_sn: Optional[int] = None
    customer_name: str
    facility_name: str
    location_name: str
    equipment_type: str
    location_timezone: str
    data_start_epoch: int
    equipment_start_epoch: int
    work_cycle: bool
    np_voltage: float
    np_current: float
    np_frequency: float
    np_hp: Optional[float] = None
    np_rpm: Optional[float] = None
    np_poles: Optional[float] = None
    np_rotor_bars: Optional[float] = None
    np_stator_slots: Optional[float] = None
    # belt_frequency_scan: bool
    # belt_frequencies: Optional[list] = None
    # gearbox_frequency_scan: bool
    # gearbox_frequencies: Optional[list] = None
    # current_rise_scan: bool
    # trending_report_scan: bool
    # current_hat_report_scan: bool
    # voltage_hat_report_scan: bool
    # events_report_scan: bool
    i_noise: float
    v_noise: float
    starter: Optional[str] = None
    # motor_change_epoch: Optional[list] = None
    load_application: Optional[str] = None
    ct_location: Optional[str] = None
    v_tap_location: Optional[str] = None
    load_imbalance_pairs: Optional[list] = None
    harmonic_frequencies_scan: Optional[dict] = None
    tr_i_max: Optional[float] = None
    active_ia: Optional[bool] = None
    active_ib: Optional[bool] = None
    active_ic: Optional[bool] = None
    active_pa: Optional[bool] = None
    active_pb: Optional[bool] = None
    active_pc: Optional[bool] = None
    container: Optional[str] = None
    voltage_boundary_upper_percent: Optional[float] = None
    voltage_imbalance_boundary_upper_percent: Optional[float] = None
    voltage_imbalance_threshold: Optional[float] = None
    voltage_thd_boundary_upper_percent: Optional[float] = None
    voltage_thd_threshold: Optional[float] = None
    current_boundary_upper_percent: Optional[float] = None
    current_deployment_status: Optional[str] = None
    current_deployment_status_update_timestamp: Optional[int] = None
    current_imbalance_boundary_upper_percent: Optional[float] = None
    current_imbalance_threshold: Optional[float] = None
    current_thd_boundary_upper_percent: Optional[float] = None
    current_thd_threshold: Optional[float] = None
    deployment_issue: Optional[bool] = None
    deprecation_notice: Optional[str] = None
    disable_monitoring: Optional[bool] = None
    ip_address: Optional[str] = None
    loss_of_phase_transient_threshold: Optional[float] = None
    mac_address: Optional[str] = None
    monitor_data_point_alerts: Optional[bool] = None
    nameplate_valid: Optional[bool] = None
    new_deployment_status: Optional[str] = None
    new_deployment_status_update_timestamp: Optional[int] = None
    notify_on_connect: Optional[bool] = None
    notify_on_current_above_noise: Optional[bool] = None
    over_current_threshold: Optional[float] = None
    over_voltage_threshold: Optional[float] = None
    pause_notifications: Optional[bool] = None
    product_type: Optional[str] = None
    under_voltage_threshold: Optional[float] = None
    alert_library_flags: Optional[FaultLibraryAlertFlags] = None
    eq_type: Optional[str] = None
    eq_type_sub: Optional[str] = None
    np_sf: Optional[float] = 1.15
    facility_location: Optional[str] = None


