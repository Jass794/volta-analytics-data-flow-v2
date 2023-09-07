from dataclasses import dataclass
from typing import Optional, List, Any

from pydantic import BaseModel, validator
from sqlalchemy import Column, INTEGER, Float, VARCHAR, ARRAY, JSON, REAL, BIGINT, BOOLEAN, TEXT, NUMERIC
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


# Portal response
class Node(BaseModel):
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
    np_running_speed: float = 0
    np_poles: float = 0
    np_rotor_bars: float = 0
    np_stator_slots: float = 0
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
class EquipmentStartDate(BaseModel):
    date: int
    day: int
    hours: int
    minutes: int
    month: int
    seconds: int
    time: int
    timezoneOffset: int
    year: int


# Portal response
class Location(BaseModel):
    ctLocation: str
    customerXRefId: str
    displayName: str
    equipmentStartDate: EquipmentStartDate
    equipmentStartTimestamp: int
    equipmentType: str
    id: str
    loadApplication: str
    locationNodeIds: dict
    name: str
    nodes: List[Node]
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


# Portal response
class Facility(BaseModel):
    customerXRefId: str
    division: str
    id: str
    locations: List[Location]
    managers: List
    name: str
    siteName: str


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
    vfd_driven: bool
    work_cycle: bool
    np_voltage: float
    np_current: float
    np_frequency: float
    np_hp: Optional[float] = None
    np_rpm: Optional[int] = None
    np_poles: Optional[int] = None
    np_rotor_bars: Optional[int] = None
    np_stator_slots: Optional[int] = None
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

    class Config:
        orm_mode = True


@dataclass
class PortfolioV2(Base):
    __table_args__ = {"schema": "volta", "extend_existing": True}
    __tablename__ = "portfolio_v2"

    # id = Column(INTEGER, primary_key=False, nullable=False)
    customer_id = Column(VARCHAR, primary_key=False, nullable=False)
    customer_code = Column(VARCHAR, primary_key=False, nullable=False)
    facility_id = Column(VARCHAR, primary_key=False, nullable=False)
    location_id = Column(VARCHAR, primary_key=False, nullable=False)
    location_node_id = Column(VARCHAR, primary_key=True, nullable=False)
    node_sn = Column(INTEGER, primary_key=True, nullable=False)
    secondary_node_sn = Column(INTEGER, primary_key=False, nullable=True)
    customer_name = Column(VARCHAR, primary_key=False, nullable=False)
    facility_name = Column(VARCHAR, primary_key=False, nullable=False)
    location_name = Column(VARCHAR, primary_key=False, nullable=False)
    equipment_type = Column(VARCHAR, primary_key=False, nullable=False)
    location_timezone = Column(VARCHAR, primary_key=False, nullable=False)
    data_start_epoch = Column(INTEGER, primary_key=False, nullable=False)
    equipment_start_epoch = Column(INTEGER, primary_key=False, nullable=False)
    vfd_driven = Column(BOOLEAN, primary_key=False, nullable=False)
    work_cycle = Column(BOOLEAN, primary_key=False, nullable=False)
    np_voltage = Column(DOUBLE_PRECISION, primary_key=False, nullable=False)
    np_current = Column(DOUBLE_PRECISION, primary_key=False, nullable=False)
    np_frequency = Column(DOUBLE_PRECISION, primary_key=False, nullable=False)
    np_hp = Column(DOUBLE_PRECISION, primary_key=False, nullable=True)
    np_rpm = Column(INTEGER, primary_key=False, nullable=True)
    np_poles = Column(INTEGER, primary_key=False, nullable=True)
    np_rotor_bars = Column(INTEGER, primary_key=False, nullable=True)
    np_stator_slots = Column(INTEGER, primary_key=False, nullable=True)
    belt_frequency_scan = Column(BOOLEAN, primary_key=False, nullable=False, default=True)
    belt_frequencies = Column(ARRAY(Float), primary_key=False, nullable=True, default=[])
    gearbox_frequency_scan = Column(BOOLEAN, primary_key=False, nullable=False, default=True)
    gearbox_frequencies = Column(ARRAY(Float), primary_key=False, nullable=True, default=[])
    current_rise_scan = Column(BOOLEAN, primary_key=False, nullable=False, default=False)
    trending_report_scan = Column(BOOLEAN, primary_key=False, nullable=False, default=True)
    current_hat_report_scan = Column(BOOLEAN, primary_key=False, nullable=False, default=True)
    voltage_hat_report_scan = Column(BOOLEAN, primary_key=False, nullable=False, default=True)
    events_report_scan = Column(BOOLEAN, primary_key=False, nullable=False, default=True)
    i_noise = Column(DOUBLE_PRECISION, primary_key=False, nullable=False, default=1.0)
    v_noise = Column(DOUBLE_PRECISION, primary_key=False, nullable=False, default=30.0)
    starter = Column(VARCHAR, primary_key=False, nullable=True)
    motor_change_epoch = Column(ARRAY(INTEGER), primary_key=False, nullable=True)
    load_application = Column(VARCHAR, primary_key=False, nullable=True)
    ct_location = Column(VARCHAR, primary_key=False, nullable=True)
    v_tap_location = Column(VARCHAR, primary_key=False, nullable=True)
    load_imbalance_pairs = Column(ARRAY(VARCHAR), primary_key=False, nullable=True)
    harmonic_frequencies_scan = Column(JSON, primary_key=False, nullable=True)
    tr_i_max = Column(REAL, primary_key=False, nullable=True)
    active_ia = Column(BOOLEAN, primary_key=False, nullable=True)
    active_ib = Column(BOOLEAN, primary_key=False, nullable=True)
    active_ic = Column(BOOLEAN, primary_key=False, nullable=True)
    active_pa = Column(BOOLEAN, primary_key=False, nullable=True)
    active_pb = Column(BOOLEAN, primary_key=False, nullable=True)
    active_pc = Column(BOOLEAN, primary_key=False, nullable=True)
    container = Column(VARCHAR, primary_key=False, nullable=True)
    voltage_boundary_upper_percent = Column(Float, primary_key=False, nullable=True)
    voltage_imbalance_boundary_upper_percent = Column(Float, primary_key=False, nullable=True)
    voltage_imbalance_threshold = Column(Float, primary_key=False, nullable=True)
    voltage_thd_boundary_upper_percent = Column(Float, primary_key=False, nullable=True)
    voltage_thd_threshold = Column(Float, primary_key=False, nullable=True)
    current_boundary_upper_percent = Column(Float, primary_key=False, nullable=True)
    current_deployment_status = Column(VARCHAR, primary_key=False, nullable=True)
    current_deployment_status_update_timestamp = Column(BIGINT, primary_key=False, nullable=True)
    current_imbalance_boundary_upper_percent = Column(Float, primary_key=False, nullable=True)
    current_imbalance_threshold = Column(Float, primary_key=False, nullable=True)
    current_thd_boundary_upper_percent = Column(Float, primary_key=False, nullable=True)
    current_thd_threshold = Column(Float, primary_key=False, nullable=True)
    deployment_issue = Column(BOOLEAN, primary_key=False, nullable=True)
    deprecation_notice = Column(TEXT, primary_key=False, nullable=True)
    disable_monitoring = Column(BOOLEAN, primary_key=False, nullable=True)
    ip_address = Column(VARCHAR, primary_key=False, nullable=True)
    loss_of_phase_transient_threshold = Column(Float, primary_key=False, nullable=True)
    mac_address = Column(VARCHAR, primary_key=False, nullable=True)
    monitor_data_point_alerts = Column(BOOLEAN, primary_key=False, nullable=True)
    nameplate_valid = Column(BOOLEAN, primary_key=False, nullable=True)
    new_deployment_status = Column(VARCHAR, primary_key=False, nullable=True)
    new_deployment_status_update_timestamp = Column(BIGINT, primary_key=False, nullable=True)
    notify_on_connect = Column(BOOLEAN, primary_key=False, nullable=True)
    notify_on_current_above_noise = Column(BOOLEAN, primary_key=False, nullable=True)
    over_current_threshold = Column(Float, primary_key=False, nullable=True)
    over_voltage_threshold = Column(Float, primary_key=False, nullable=True)
    pause_notifications = Column(BOOLEAN, primary_key=False, nullable=True)
    product_type = Column(VARCHAR, primary_key=False, nullable=True)
    under_voltage_threshold = Column(Float, primary_key=False, nullable=True)


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
    vfd_driven: bool
    work_cycle: bool
    np_voltage: float
    np_current: float
    np_frequency: float
    np_hp: Optional[float] = None
    np_rpm: Optional[int] = None
    np_poles: Optional[int] = None
    np_rotor_bars: Optional[int] = None
    np_stator_slots: Optional[int] = None
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
