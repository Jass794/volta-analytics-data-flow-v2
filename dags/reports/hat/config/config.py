from volta_analytics_db_models.tabels.portfolio_v2_models import PortfolioV2Model
from reports.hat.models.configs import HatConfigsModel


class HatConfigs:
    
    def get_hat_configs(location: PortfolioV2Model):

        # Configs for the scan scan duration
        st_scan_look_back_days = 15  # Number of days used by scan to find max and min for filter
        st_scan_lt_avg_days = 15  # Number of days used by scan for long term average

        lt_scan_look_back_days = 15  # Number of days used by scan to find max and min for filter
        # Change days for VFD
        if location.starter == 'VFD':
            st_scan_lt_avg_days = 30
            st_scan_look_back_days = 30
            lt_scan_look_back_days = 30
        

        hat_configs  = HatConfigsModel(lt_data_days=90,
                                        st_scan_lt_avg_days=st_scan_lt_avg_days,
                                        st_scan_look_back_days=st_scan_look_back_days,
                                        lt_scan_look_back_days=lt_scan_look_back_days, # Number of days used by scan to find max and min for filter
                                        lt_scan_lt_avg_days=90,  # Number of days used by scan for long term average
                                        lt_scan_st_avg_days=3,  # short term average period
                                        st_scan_st_avg_days=3)
        
        return hat_configs