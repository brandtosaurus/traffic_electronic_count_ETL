import queries as q
import config
import main
import pandas as pd
import numpy as np
import uuid
import pdb
import traceback
from typing import List

ADD_NEW_COLUMNS_TO_HSWIM_HEADER = """
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_4tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_4tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_4tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_4tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_4tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_4tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_4tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_4tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_4tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_4tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_5tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_5tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_5tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_5tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_5tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_5tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_5tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_5tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_5tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_5tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_6tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_6tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_6tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_6tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_6tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_6tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_6tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_6tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_6tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_6tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_7tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_7tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_7tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_7tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_7tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_7tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_7tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_7tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_7tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_7tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_8tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_8tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_8tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_8tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_8tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_8tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_8tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_8tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_8tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_8tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_9tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_9tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_9tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_9tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_9tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_9tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_9tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_9tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_9tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_9tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_10tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_10tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_10tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_10tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_10tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_10tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_10tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_10tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_10tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_10tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_11tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_11tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_11tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_11tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_11tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_11tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_11tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_11tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_11tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_11tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_12tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_12tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_12tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_12tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_12tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_12tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_12tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_12tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_12tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_12tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_13tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_13tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_13tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_13tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_13tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_13tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_13tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_13tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_13tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_13tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_14tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_14tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_14tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_14tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_14tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_14tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_14tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_14tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_14tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_14tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_15tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_15tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_15tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_15tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_15tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_15tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_15tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_15tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_15tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_15tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_16tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_16tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_16tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_16tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_16tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_16tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_16tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_16tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_16tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_16tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_17tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_17tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_17tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_17tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_17tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_17tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_17tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_17tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_17tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_17tot numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_4p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_4p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_4p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_4p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_4p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_4p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_4p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_4p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_4p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_4p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_5p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_5p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_5p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_5p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_5p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_5p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_5p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_5p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_5p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_5p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_6p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_6p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_6p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_6p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_6p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_6p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_6p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_6p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_6p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_6p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_7p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_7p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_7p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_7p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_7p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_7p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_7p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_7p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_7p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_7p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_8p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_8p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_8p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_8p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_8p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_8p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_8p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_8p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_8p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_8p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_9p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_9p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_9p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_9p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_9p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_9p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_9p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_9p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_9p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_9p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_10p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_10p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_10p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_10p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_10p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_10p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_10p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_10p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_10p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_10p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_11p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_11p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_11p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_11p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_11p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_11p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_11p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_11p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_11p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_11p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_12p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_12p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_12p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_12p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_12p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_12p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_12p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_12p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_12p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_12p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_13p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_13p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_13p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_13p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_13p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_13p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_13p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_13p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_13p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_13p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_14p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_14p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_14p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_14p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_14p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_14p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_14p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_14p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_14p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_14p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_15p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_15p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_15p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_15p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_15p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_15p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_15p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_15p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_15p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_15p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_16p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_16p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_16p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_16p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_16p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_16p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_16p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_16p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_16p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_16p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_17p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_17p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_17p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_17p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_17p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_17p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_17p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_17p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_17p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_17p numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_4n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_4n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_4n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_4n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_4n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_4n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_4n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_4n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_4n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_4n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_5n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_5n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_5n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_5n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_5n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_5n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_5n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_5n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_5n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_5n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_6n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_6n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_6n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_6n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_6n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_6n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_6n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_6n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_6n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_6n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_7n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_7n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_7n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_7n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_7n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_7n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_7n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_7n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_7n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_7n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_8n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_8n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_8n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_8n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_8n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_8n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_8n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_8n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_8n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_8n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_9n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_9n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_9n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_9n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_9n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_9n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_9n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_9n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_9n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_9n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_10n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_10n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_10n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_10n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_10n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_10n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_10n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_10n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_10n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_10n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_11n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_11n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_11n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_11n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_11n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_11n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_11n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_11n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_11n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_11n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_12n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_12n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_12n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_12n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_12n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_12n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_12n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_12n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_12n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_12n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_13n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_13n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_13n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_13n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_13n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_13n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_13n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_13n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_13n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_13n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_14n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_14n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_14n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_14n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_14n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_14n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_14n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_14n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_14n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_14n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_15n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_15n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_15n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_15n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_15n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_15n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_15n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_15n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_15n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_15n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_16n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_16n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_16n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_16n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_16n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_16n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_16n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_16n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_16n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_16n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_vehicles_cls_17n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists weighed_vehicles_cls_17n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists perc_truck_dist_cls_17n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_axles_cls_17n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists axles_over9t_cls_17n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_mass_cls_17n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists cnt_mass_over9t_cls_17n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists eal_pervehicle_cls_17n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_cls_17n numeric;
alter table trafc.electronic_count_header_hswim add column if not exists total_eal_over9t_cls_17n numeric;
"""

class Wim():
    def __init__(self, data, head_df, header_id, site_id, pt_cols) -> None:
        self.header_id = header_id
        self.site_id = site_id
        self.data_df = data
        self.head_df = head_df
        self.pt_cols = pt_cols
        if self.head_df is None:
            pass
        else:
            self.primary_vehicle_class = int(
                self.head_df.loc[self.head_df[0] == "10", 1].values[0])
            self.secondary_vehicle_class = int(
                self.head_df.loc[self.head_df[0] == "10", 2].values[0])
        self.header_ids = self.get_headers_to_update()
        self.t10_cols = list(pd.read_sql_query(
            q.SELECT_ELECTRONIC_COUNT_DATA_TYPE_10_LIMIT1, config.ENGINE).columns)
        self.wim_header_cols = list(pd.read_sql_query(
            q.SEELECT_HSWIM_HEADER_COLS, config.ENGINE).columns)
        self.vm_limits = pd.read_sql_query(f"SELECT * FROM {config.TRAFFIC_LOOKUP_SCHEMA}.gross_vehicle_mass_limits;", config.ENGINE)

    def type_10(self) -> pd.DataFrame:
        """
        It takes a dataframe, checks if it's empty, if not, it checks if it's empty, if not, it checks
        if it's empty, if not, it does some stuff, then it checks if it's empty, if not, it does some
        stuff, then it checks if it's empty, if not, it does some stuff, then it checks if it's empty,
        if not, it does some stuff, then it checks if it's empty, if not, it does some stuff, then it
        checks if it's empty, if not, it does some stuff, then it checks if it's empty, if not, it does
        some stuff, then it checks if it's empty, if not, it does some stuff, then it checks if it's
        empty, if not, it does some stuff, then it checks if it's empty, if not, it does some stuff,
        then it checks if it's empty, if not, it does
        :return: A tuple of two dataframes.
        """
        if self.data_df is None:
            pass
        else:
            if self.data_df.empty:
                return None
            elif self.data is None:
                return None
            else:
                data = self.data_df.loc[(self.data_df[0] == "10")].dropna(
                    axis=1, how="all"
                ).reset_index(drop=True).copy()

                num_of_fields = int(data.iloc[:, 1].unique()[0])
                ddf = data.iloc[:, : 2 + num_of_fields]
                ddf.rename(columns=config.RENAME_TYPE10_DATA_COLUMNS,
                           inplace=True, errors='ignore')
                ddf["data_id"] = ddf.apply(lambda x: uuid.uuid4(), axis=1)

                if data.shape[1] > ddf.shape[1]:
                    sub_data_df = pd.DataFrame(columns=[
                        'sub_data_type_code',
                        'offset_sensor_detection_code',
                        'mass_measurement_resolution_kg',
                        'number',
                        'value'])
                    for index, row in data.iterrows():
                        col = int(row[1]) + 2
                        while col < len(row) - 5 and row[col] is not None:
                            sub_data_type = row[col]
                            col += 1
                            no_of_type = int(row[col])
                            col += 1
                            if sub_data_type[0].lower() in ['w', 'a', 'g']:
                                odc = row[col]
                                col += 1
                                mmr = row[col]
                                col += 1
                                for i in range(0, no_of_type):
                                    tempdf = pd.DataFrame([[
                                        sub_data_type,
                                        odc,
                                        mmr,
                                        i + 1,
                                        row[col]]
                                    ], columns=[
                                        'sub_data_type_code',
                                        'offset_sensor_detection_code',
                                        'mass_measurement_resolution_kg',
                                        'number',
                                        'value'
                                    ])
                                    sub_data_df = pd.concat(
                                        [sub_data_df, tempdf])
                                    col += 1
                            elif sub_data_type[0].lower() in ['s', 't', 'c']:
                                for i in range(0, no_of_type):
                                    tempdf = pd.DataFrame([[
                                        sub_data_type,
                                        i + 1,
                                        row[col]]
                                    ], columns=[
                                        'sub_data_type_code',
                                        'number',
                                        'value'])
                                    sub_data_df = pd.concat(
                                        [sub_data_df, tempdf])
                                    col += 1
                            elif sub_data_type[0].lower() in ['v']:
                                odc = row[col]
                                col += 1
                                for i in range(0, no_of_type):
                                    tempdf = pd.DataFrame([[
                                        sub_data_type,
                                        odc,
                                        i + 1,
                                        row[col]]
                                    ], columns=[
                                        'sub_data_type_code',
                                        'offset_sensor_detection_code',
                                        'number',
                                        'value'
                                    ])
                                    sub_data_df = pd.concat(
                                        [sub_data_df, tempdf])
                                    col += 1
                else:
                    sub_data_df = pd.DataFrame(columns=[
                        'sub_data_type_code',
                        'offset_sensor_detection_code',
                        'mass_measurement_resolution_kg',
                        'number',
                        'value'])

                sub_data_df = sub_data_df.merge(
                    ddf['data_id'], how='left', left_index=True, right_index=True)

                ddf = ddf.fillna(0)
                ddf["assigned_lane_number"] = ddf["assigned_lane_number"].astype(
                    int)
                ddf["lane_number"] = ddf["physical_lane_number"].astype(int)
                ddf["physical_lane_number"] = ddf["physical_lane_number"].astype(
                    int)

                ddf = ddf.replace(r'^\s*$', np.NaN, regex=True)
                sub_data_df = sub_data_df.replace(r'^\s*$', np.NaN, regex=True)
                sub_data_df = sub_data_df.drop(
                    "index", axis=1, errors='ignore')

                scols = ddf.select_dtypes('object').columns
                ddf[scols] = ddf[scols].apply(
                    pd.to_numeric, axis=1, errors='ignore')

                ddf['year'] = ddf['start_datetime'].dt.year
                ddf["site_id"] = self.site_id
                ddf["header_id"] = self.header_id

                ddf = ddf[ddf.columns.intersection(self.t10_cols)]

                return ddf, sub_data_df

    def wim_header_calcs(self, df: pd.DataFrame, df2: pd.DataFrame, df3: pd.DataFrame):
        """
        df1 is the SELECT_TYPE10_QRY
        df2 is the AXLE_SPACING_SELECT_QRY
        df3 is the WHEEL_MASS_SELECT_QRY
        """

        try:
            self.egrl_percent = 1-(
                ((df.loc[df['edit_code'] == 2].count()[0])/(df.count()[0])))
        except:
            self.egrl_percent = 1
        try:
            self.egrl_percent_positive_direction = 1-((df.loc[(df['edit_code'] == 2) & (
                df['direction'] == 'P')].count()[0]/df.loc[df['direction'] == 'P'].count()[0]))
        except:
            self.egrl_percent_positive_direction = 1
        try:
            self.egrl_percent_negative_direction = 1-((df.loc[(df['edit_code'] == 2) & (
                df['direction'] == 'P')].count()[0]/df.loc[df['direction'] == 'N'].count()[0]))
        except:
            self.egrl_percent_negative_direction = 1
        try:
            self.egrw_percent = 1-(((df2.loc[df2['edit_code'] == 2].count(
            )[0]+df3.loc[df3['edit_code'] == 2].count()[0])/df.count()[0]))
        except:
            self.egrw_percent = 0
        try:
            self.egrw_percent_positive_direction = 1-(((df2.loc[(df2['edit_code'] == 2) & (df2['direction'] == 'P')].count(
            )[0]+df3.loc[(df3['edit_code'] == 2) & (df3['direction'] == 'P')].count()[0])/df.loc[df['direction'] == 'P'].count()[0]))
        except:
            self.egrw_percent_positive_direction = 1
        try:
            self.egrw_percent_negative_direction = 1-(((df2.loc[(df2['edit_code'] == 2) & (df2['direction'] == 'N')].count(
            )[0]+df3.loc[(df3['edit_code'] == 2) & (df3['direction'] == 'N')].count()[0])/df.loc[df['direction'] == 'N'].count()[0]))
        except:
            self.egrw_percent_negative_direction = 1

        self.num_weighed = df3.groupby(
            pd.Grouper(key='id')).count().count()[0] or 0
        self.num_weighed_positive_direction = df3.loc[df3['direction'] == 'P'].groupby(
            pd.Grouper(key='id')).count().count()[0] or 0
        self.num_weighed_negative_direction = df3.loc[df3['direction'] == 'N'].groupby(
            pd.Grouper(key='id')).count().count()[0] or 0
        self.mean_equivalent_axle_mass = round(
            (df3.groupby(pd.Grouper(key='id'))['wheel_mass'].mean()).mean(), 2) or 0
        self.mean_equivalent_axle_mass_positive_direction = round(
            (df3.loc[df3['direction'] == 'P'].groupby(pd.Grouper(key='id'))['wheel_mass'].mean()).mean(), 2) or 0
        self.mean_equivalent_axle_mass_negative_direction = round(
            (df3.loc[df3['direction'] == 'N'].groupby(pd.Grouper(key='id'))['wheel_mass'].mean()).mean(), 2) or 0
        self.mean_axle_spacing = round((df2.groupby(pd.Grouper(key='id')).mean()).mean()[
                                       'axle_spacing_number'], 0) or 0
        self.mean_axle_spacing_positive_direction = round((df2.loc[df2['direction'] == 'P'].groupby(
            pd.Grouper(key='id')).mean()).mean()['axle_spacing_number'], 0) or 0
        self.mean_axle_spacing_negative_direction = round((df2.loc[df2['direction'] == 'N'].groupby(
            pd.Grouper(key='id')).mean()).mean()['axle_spacing_number'], 0) or 0

        self.e80_per_axle = ((df3['wheel_mass']/8200)**4.2).sum() or 0

        self.e80_per_axle_positive_direction = (
            (df3.loc[df3['direction'] == 'P']['wheel_mass']/8200)**4.2).sum() or 0

        self.e80_per_axle_negative_direction = (
            (df3.loc[df3['direction'] == 'N']['wheel_mass']/8200)**4.2).sum() or 0

        self.olhv = len(
            df3.loc[df3['gross_mass'] > df3['vehicle_mass_limit_kg']]['id'].unique()) or 0
        self.olhv_positive_direction = len(df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
            df3['direction'] == 'P')]['id'].unique()) or 0
        self.olhv_negative_direction = len(df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
            df3['direction'] == 'N')]['id'].unique()) or 0
        try:
            self.olhv_percent = (
                (len(df3.loc[df3['gross_mass'] > df3['vehicle_mass_limit_kg']]['id'].unique())/len(df3['id'].unique())))
        except ZeroDivisionError:
            self.olhv_percent = 0
        try:
            self.olhv_percent_positive_direction = ((len(df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
                df3['direction'] == 'P')]['id'].unique())/len(df3.loc[df3['direction'] == 'P']['id'].unique())))
        except ZeroDivisionError:
            self.olhv_percent_positive_direction = 0
        try:
            self.olhv_percent_negative_direction = ((len(df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
                df3['direction'] == 'N')]['id'].unique())/len(df3.loc[df3['direction'] == 'N']['id'].unique())))
        except ZeroDivisionError:
            self.olhv_percent_negative_direction = 0
        self.tonnage_generated = (
            (df3['wheel_mass']).sum()/1000).round().astype(int) or 0
        self.tonnage_generated_positive_direction = (
            (df3.loc[df3['direction'] == 'P']['wheel_mass']).sum()/1000).round().astype(int) or 0
        self.tonnage_generated_negative_direction = (
            (df3.loc[df3['direction'] == 'N']['wheel_mass']).sum()/1000).round().astype(int) or 0
        self.olton = (df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg'])]
                      ['wheel_mass']).sum().astype(int) or 0
        self.olton_positive_direction = (df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
            df3['direction'] == 'P')]['wheel_mass']).sum().astype(int) or 0
        self.olton_negative_direction = (df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
            df3['direction'] == 'N')]['wheel_mass']).sum().astype(int) or 0
        try:
            self.olton_percent = ((df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg'])]
                                   ['wheel_mass']).sum().round(4)/(df3['wheel_mass']).sum().round(4))
        except ZeroDivisionError:
            self.olton_percent = 0
        try:
            self.olton_percent_positive_direction = ((df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
                df3['direction'] == 'P')]['wheel_mass']).sum().round(4)/(df3.loc[df3['direction'] == 'P']['wheel_mass']).sum().round(4))
        except ZeroDivisionError:
            self.olton_percent_positive_direction = 0
        try:
            self.olton_percent_negative_direction = ((df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
                df3['direction'] == 'N')]['wheel_mass']).sum().round(4)/(df3.loc[df3['direction'] == 'N']['wheel_mass']).sum().round(4))
        except ZeroDivisionError:
            self.olton_percent_negative_direction = 0
        self.ole80 = round(
            ((df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg'])]['wheel_mass']/8200)**4.2).sum(), 0) or 0
        self.ole80_positive_direction = round(((df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
            df3['direction'] == 'P')]['wheel_mass']/8200)**4.2).sum(), 0) or 0
        self.ole80_negative_direction = round(((df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
            df3['direction'] == 'N')]['wheel_mass']/8200)**4.2).sum(), 0) or 0

        try:
            self.ole80_percent = ((df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg'])]['wheel_mass']/8200)**4.2).sum(
            )/((df3['wheel_mass']/8200)**4.2).sum()
        except ZeroDivisionError:
            self.ole80_percent = 0

        try:
            self.ole80_percent_positive_direction = ((((df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
                df3['direction'] == 'P')]['wheel_mass']/8200)**4.2).sum().round(4)/((df3.loc[df3['direction'] == 'P']['wheel_mass']/8200)**4.2).sum().round())).round(4)
        except ZeroDivisionError:
            self.ole80_percent_positive_direction = 0

        try:
            self.ole80_percent_negative_direction = ((((df3.loc[(df3['gross_mass'] > df3['vehicle_mass_limit_kg']) & (
                df3['direction'] == 'N')]['wheel_mass']/8200)**4.2).sum().round(4)/((df3.loc[df3['direction'] == 'N']['wheel_mass']/8200)**4.2).sum().round())).round(4)
        except ZeroDivisionError:
            self.ole80_percent_negative_direction = 0

        self.xe80 = round(((df3['wheel_mass']/8200)**4.2).sum() -
                          (((df3['wheel_mass']-(9000*0.05))/8200)**4.2).sum())

        self.xe80_positive_direction = round(((df3.loc[df3['direction'] == 'P']['wheel_mass']/8200)**4.2).sum()-(
            ((df3.loc[df3['direction'] == 'P']['wheel_mass']-(9000*0.05))/8200)**4.2).sum()) or 0

        self.xe80_negative_direction = round(((df3.loc[df3['direction'] == 'N']['wheel_mass']/8200)**4.2).sum()-(
            ((df3.loc[df3['direction'] == 'N']['wheel_mass']-(9000*0.05))/8200)**4.2).sum()) or 0

        try:
            self.xe80_percent = ((((df3['wheel_mass']/8200)**4.2).sum()-(
                ((df3['wheel_mass'])*(9000*0.05)/8200)**4.2).sum())/((df3['wheel_mass']/8200)**4.2).sum()).round(4)
        except ZeroDivisionError:
            self.xe80_percent = 0

        try:
            self.xe80_percent_positive_direction = (((((df3.loc[df3['direction'] == 'P']['wheel_mass']/8200)**4.2).sum()-(
                ((df3.loc[df3['direction'] == 'P']['wheel_mass'])*(9000*0.05)/8200)**4.2).sum())/((df3.loc[df3['direction'] == 'P']['wheel_mass']/8200)**4.2).sum())).round(4)
        except ZeroDivisionError:
            self.xe80_percent_positive_direction = 0

        try:
            self.xe80_percent_negative_direction = (((((df3.loc[df3['direction'] == 'N']['wheel_mass']/8200)**4.2).sum()-(
                ((df3.loc[df3['direction'] == 'N']['wheel_mass'])*(9000*0.05)/8200)**4.2).sum())/((df3.loc[df3['direction'] == 'N']['wheel_mass']/8200)**4.2).sum())).round(4)
        except ZeroDivisionError:
            self.xe80_percent_negative_direction = 0

        try:
            self.e80_per_day = ((((df3['wheel_mass']/8200)**4.2).sum().round()/df3.groupby(
                pd.Grouper(key='start_datetime', freq='D')).count().count()[0])).round(4)
        except ZeroDivisionError:
            self.e80_per_day = 0

        try:
            self.e80_per_day_positive_direction = ((((df3.loc[df3['direction'] == 'P']['wheel_mass']/8200)**4.2).sum().round(
            )/df3.loc[df3['direction'] == 'P'].groupby(pd.Grouper(key='start_datetime', freq='D')).count().count()[0])).round(4)
        except ZeroDivisionError:
            self.e80_per_day_positive_direction = 0
        try:
            self.e80_per_day_negative_direction = ((((df3.loc[df3['direction'] == 'N']['wheel_mass']/8200)**4.2).sum().round(
            )/df3.loc[df3['direction'] == 'N'].groupby(pd.Grouper(key='start_datetime', freq='D')).count().count()[0])).round(4)
        except ZeroDivisionError:
            self.e80_per_day_negative_direction = 0
        try:
            self.e80_per_heavy_vehicle = ((((df3.loc[df3['vehicle_class_code_primary_scheme'] > 3]['wheel_mass']/8200)**4.2).sum(
            ).round()/df3.loc[df3['vehicle_class_code_primary_scheme'] > 3].count()[0])).round(4)
        except ZeroDivisionError:
            self.e80_per_heavy_vehicle = 0
        try:
            self.e80_per_heavy_vehicle_positive_direction = ((((df3.loc[(df3['vehicle_class_code_primary_scheme'] > 3) & (
                df3['direction'] == 'P')]['wheel_mass']/8200)**4.2).sum().round()/df3.loc[(df3['vehicle_class_code_primary_scheme'] > 3) & (df3['direction'] == 'P')].count()[0])).round(4)
        except ZeroDivisionError:
            self.e80_per_heavy_vehicle_positive_direction = 0
        try:
            self.e80_per_heavy_vehicle_negative_direction = ((((df3.loc[(df3['vehicle_class_code_primary_scheme'] > 3) & (
                df3['direction'] == 'N')]['wheel_mass']/8200)**4.2).sum().round()/df3.loc[(df3['vehicle_class_code_primary_scheme'] > 3) & (df3['direction'] == 'N')].count()[0])).round(4)
        except ZeroDivisionError:
            self.e80_per_heavy_vehicle_negative_direction = 0
            
        # the below calculates the number of axle groups
        try:
            nr_axle_groups = df2.loc[df2.axle_spacing_cm <= 220].groupby(df2.id)['id'].count()
            nr_axle_groups.name = 'nr_axle_groups'
            df2 = df2.drop(columns=['nr_axle_groups'])
            df2 = df2.join(nr_axle_groups, on='id')
        except:
            pass

        
        # The below gets the steering single axle vehicles.
        mask = df2['id'].isin(df2.loc[(df2['axle_spacing_number'] == 1) & (df2['axle_spacing_cm'] > 220), 'id'].tolist())
        steering_single_axles = df2.loc[mask].copy()
        steering_single_axles_per_dir = steering_single_axles.loc[steering_single_axles.wheel_mass_number <= 2].groupby(['id','direction']).sum()
        steering_single_axles = steering_single_axles.loc[steering_single_axles.wheel_mass_number <= 2].groupby('id').sum()

        self.worst_steering_single_axle_cnt = steering_single_axles.loc[steering_single_axles.wheel_mass > 7500].count()
        self.worst_steering_single_axle_olhv_perc = steering_single_axles.loc[steering_single_axles.wheel_mass > 7500].count() / steering_single_axles.count()
        self.worst_steering_single_axle_tonperhv = steering_single_axles.loc[steering_single_axles.wheel_mass > 7500].sum() / steering_single_axles.loc[steering_single_axles.wheel_mass > 7500].count()
        try:
            self.worst_steering_single_axle_cnt_positive_direciton = steering_single_axles_per_dir.loc[(steering_single_axles_per_dir['direction'] == 'P') & (steering_single_axles_per_dir.wheel_mass > 7500)].count()
        except:
            self.worst_steering_single_axle_cnt_positive_direciton = 0
        try:
            self.worst_steering_single_axle_olhv_perc_positive_direciton = steering_single_axles_per_dir.loc[(steering_single_axles_per_dir['direction'] == 'P') & (steering_single_axles_per_dir.wheel_mass > 7500)].count() / steering_single_axles_per_dir.loc[(steering_single_axles_per_dir['direction'] == 'P')].count()
        except:
            self.worst_steering_single_axle_olhv_perc_positive_direciton = 0
        try:
            self.worst_steering_single_axle_tonperhv_positive_direciton = steering_single_axles_per_dir.loc[(steering_single_axles_per_dir['direction'] == 'P') & (steering_single_axles_per_dir.wheel_mass > 15000)].sum() / steering_single_axles_per_dir.loc[(steering_single_axles_per_dir['direction'] == 'P') & (steering_single_axles_per_dir.wheel_mass > 7500)].count()
        except:
            self.worst_steering_single_axle_tonperhv_positive_direciton = 0
        try:
            self.worst_steering_single_axle_cnt_negative_direciton = steering_single_axles_per_dir.loc[(steering_single_axles_per_dir['direction'] == 'N') & (steering_single_axles_per_dir.wheel_mass > 7500)].count()
        except:
            self.worst_steering_single_axle_cnt_negative_direciton = 0
        try:
            self.worst_steering_single_axle_olhv_perc_negative_direciton = steering_single_axles_per_dir.loc[(steering_single_axles_per_dir['direction'] == 'N') & (steering_single_axles_per_dir.wheel_mass > 7500)].count() / steering_single_axles_per_dir.loc[(steering_single_axles_per_dir['direction'] == 'N')].count()
        except:
            self.worst_steering_single_axle_olhv_perc_negative_direciton = 0
        try:
            self.worst_steering_single_axle_tonperhv_negative_direciton = steering_single_axles_per_dir.loc[(steering_single_axles_per_dir['direction'] == 'N') & (steering_single_axles_per_dir.wheel_mass > 15000)].sum() / steering_single_axles_per_dir.loc[(steering_single_axles_per_dir['direction'] == 'N') & (steering_single_axles_per_dir.wheel_mass > 7500)].count()
        except:
            self.worst_steering_single_axle_tonperhv_negative_direciton = 0

        
        # The below gets the steering double axle vehicles.
        mask = df2['id'].isin(df2.loc[(df2['axle_spacing_number'] == 1) & (df2['axle_spacing_cm'] <= 220), 'id'].tolist())
        steering_double_axles = df2.loc[mask].copy()
        steering_double_axles_per_dir = steering_double_axles.loc[steering_double_axles.wheel_mass_number <= 2].groupby(['id','direction']).sum()
        steering_double_axles = steering_double_axles.loc[steering_double_axles.wheel_mass_number <= 2].groupby('id').sum()

        self.worst_steering_double_axle_cnt = steering_double_axles.loc[steering_double_axles.wheel_mass > 15000].count()
        self.worst_steering_double_axle_olhv_perc = steering_double_axles.loc[steering_double_axles.wheel_mass > 15000].count() / steering_double_axles.count()
        self.worst_steering_double_axle_tonperhv = steering_double_axles.loc[steering_double_axles.wheel_mass > 15000].sum() / steering_double_axles.loc[steering_double_axles.wheel_mass > 15000].count()
        try:
            self.worst_steering_double_axle_cnt_positive_direciton = steering_double_axles_per_dir.loc[(steering_double_axles_per_dir['direction'] == 'P') & (steering_double_axles.wheel_mass > 15000)].count()
        except:
            self.worst_steering_double_axle_cnt_positive_direciton = 0 
        try:
            self.worst_steering_double_axle_olhv_perc_positive_direciton = steering_double_axles_per_dir.loc[(steering_double_axles_per_dir['direction'] == 'P') & (steering_double_axles.wheel_mass > 15000)].count() / steering_double_axles_per_dir.loc[(steering_double_axles_per_dir['direction'] == 'P')].count()
        except:
            self.worst_steering_double_axle_olhv_perc_positive_direciton = 0 
        try:
            self.worst_steering_double_axle_tonperhv_positive_direciton = steering_double_axles_per_dir.loc[(steering_double_axles_per_dir['direction'] == 'P') & (steering_double_axles.wheel_mass > 15000)].sum() / steering_double_axles_per_dir.loc[(steering_double_axles_per_dir['direction'] == 'P') & (steering_double_axles_per_dir.wheel_mass > 15000)].count()
        except:
            self.worst_steering_double_axle_tonperhv_positive_direciton = 0 
        try:
            self.worst_steering_double_axle_cnt_negative_direciton = steering_double_axles_per_dir.loc[(steering_double_axles_per_dir['direction'] == 'N') & (steering_double_axles_per_dir.wheel_mass > 15000)].count()
        except:
            self.worst_steering_double_axle_cnt_negative_direciton = 0 
        try:
            self.worst_steering_double_axle_olhv_perc_negative_direciton = steering_double_axles_per_dir.loc[(steering_double_axles_per_dir['direction'] == 'N') & (steering_double_axles_per_dir.wheel_mass > 15000)].count() / steering_double_axles_per_dir.loc[(steering_double_axles_per_dir['direction'] == 'N')].count()
        except:
            self.worst_steering_double_axle_olhv_perc_negative_direciton = 0 
        try:
            self.worst_steering_double_axle_tonperhv_negative_direciton = steering_double_axles_per_dir.loc[(steering_double_axles_per_dir['direction'] == 'N') & (steering_double_axles_per_dir.wheel_mass > 15000)].sum() / steering_double_axles_per_dir.loc[(steering_double_axles_per_dir['direction'] == 'N') & (steering_double_axles_per_dir.wheel_mass > 15000)].count()
        except:
            self.worst_steering_double_axle_tonperhv_negative_direciton = 0 
        

        # The below gets the non-steering single axle vehicles.
        mask = df2['id'].isin(df2.loc[(df2['axle_spacing_number'] == 1) & (df2['axle_spacing_cm'] > 220) & (df2['vehicle_class_code_primary_scheme'].isin([4,5,7])), 'id'].tolist())
        non_steering_single_axles = df2.loc[mask].copy()
        non_steering_single_axles_per_dir = non_steering_single_axles.loc[non_steering_single_axles.wheel_mass_number > 1].groupby(['id','direction']).sum()
        non_steering_single_axles = non_steering_single_axles.loc[non_steering_single_axles.wheel_mass_number > 1].groupby('id').sum()

        self.worst_non_steering_single_axle_cnt = non_steering_single_axles.loc[non_steering_single_axles.wheel_mass > 9000].count()
        self.worst_non_steering_single_axle_olhv_perc = non_steering_single_axles.loc[non_steering_single_axles.wheel_mass > 9000].count() / non_steering_single_axles.count()
        self.worst_non_steering_single_axle_tonperhv = non_steering_single_axles.loc[non_steering_single_axles.wheel_mass > 9000].sum() / non_steering_single_axles.loc[non_steering_single_axles.wheel_mass > 9000].count()
        try:
            self.worst_non_steering_single_axle_cnt_positive_direciton = non_steering_single_axles_per_dir.loc[(non_steering_single_axles_per_dir['direction'] == 'P') & (non_steering_single_axles_per_dir.wheel_mass > 9000)].count()
        except:
            self.worst_non_steering_single_axle_cnt_positive_direciton = 0
        try:
            self.worst_non_steering_single_axle_olhv_perc_positive_direciton = non_steering_single_axles_per_dir.loc[(non_steering_single_axles_per_dir['direction'] == 'P') & (non_steering_single_axles_per_dir.wheel_mass > 9000)].count() / non_steering_single_axles_per_dir.loc[(non_steering_single_axles_per_dir['direction'] == 'P')].count()
        except:
            self.worst_non_steering_single_axle_olhv_perc_positive_direciton = 0
        try:
            self.worst_non_steering_single_axle_tonperhv_positive_direciton = non_steering_single_axles_per_dir.loc[(non_steering_single_axles_per_dir['direction'] == 'P') & (non_steering_single_axles_per_dir.wheel_mass > 9000)].sum() / non_steering_single_axles_per_dir.loc[(non_steering_single_axles_per_dir['direction'] == 'P') & (non_steering_single_axles_per_dir.wheel_mass > 9000)].count()
        except:
            self.worst_non_steering_single_axle_tonperhv_positive_direciton = 0
        try:
            self.worst_non_steering_single_axle_cnt_negative_direciton = non_steering_single_axles_per_dir.loc[(non_steering_single_axles_per_dir['direction'] == 'N') & (non_steering_single_axles_per_dir.wheel_mass > 9000)].count()
        except:
            self.worst_non_steering_single_axle_cnt_negative_direciton = 0
        try:
            self.worst_non_steering_single_axle_olhv_perc_negative_direciton = non_steering_single_axles_per_dir.loc[(non_steering_single_axles_per_dir['direction'] == 'N') & (non_steering_single_axles_per_dir.wheel_mass > 9000)].count() / non_steering_single_axles_per_dir.loc[(non_steering_single_axles_per_dir['direction'] == 'N')].count()
        except:
            self.worst_non_steering_single_axle_olhv_perc_negative_direciton = 0
        try:
            self.worst_non_steering_single_axle_tonperhv_negative_direciton = non_steering_single_axles_per_dir.loc[(non_steering_single_axles_per_dir['direction'] == 'N') & (non_steering_single_axles_per_dir.wheel_mass > 9000)].sum() / non_steering_single_axles_per_dir.loc[(non_steering_single_axles_per_dir['direction'] == 'N') & (non_steering_single_axles_per_dir.wheel_mass > 9000)].count()
        except:
            self.worst_non_steering_single_axle_tonperhv_negative_direciton = 0

        # The below gets the non-steering double axle vehicles.
        # mask = df2['id'].isin(df2.loc[(df2['axle_spacing_number'] == 1) & (df2['axle_spacing_cm'] > 220) & (df2['vehicle_class_code_primary_scheme'].isin([6,8,9,10,11,12,13,14,15,16,17])), 'id'].tolist())
        # non_steering_double_axles = df2.loc[mask].copy()
        # non_steering_double_axles_per_dir = non_steering_double_axles.loc[non_steering_double_axles.wheel_mass_number > 1].groupby(['id','direction']).sum()
        # non_steering_double_axles = non_steering_double_axles.loc[non_steering_double_axles.wheel_mass_number > 1].groupby('id').sum()
        
        # TODO:
        self.worst_non_steering_double_axle_cnt = 0
        self.worst_non_steering_double_axle_olhv_perc = 0
        self.worst_non_steering_double_axle_tonperhv = 0
        self.worst_non_steering_double_axle_cnt_positive_direciton = 0
        self.worst_non_steering_double_axle_olhv_perc_positive_direciton = 0
        self.worst_non_steering_double_axle_tonperhv_positive_direciton = 0
        self.worst_non_steering_double_axle_cnt_negative_direciton = 0
        self.worst_non_steering_double_axle_olhv_perc_negative_direciton = 0
        self.worst_non_steering_double_axle_tonperhv_negative_direciton = 0
        
        # The below gets the triple axle vehicles.
        # mask = df2['id'].isin(df2.loc[(df2['axle_spacing_number'] > 1) & (df2['axle_spacing_cm'] <= 220), 'id'].tolist())
        # triple_axles = df2.loc[mask].copy()
        # # triple_axles['check'] = triple_axles['axle_spacing_number'].diff().eq(1).any(axis=1).astype(int)
        # triple_axles_per_dir = triple_axles.loc[triple_axles.wheel_mass_number <= 2].groupby(['id','direction']).sum()
        # triple_axles = triple_axles.loc[triple_axles.wheel_mass_number <= 2].groupby('id').sum()

        # TODO:
        self.worst_triple_axle_cnt = 0
        self.worst_triple_axle_olhv_perc = 0
        self.worst_triple_axle_tonperhv = 0
        self.worst_triple_axle_cnt_positive_direciton = 0
        self.worst_triple_axle_olhv_perc_positive_direciton = 0
        self.worst_triple_axle_tonperhv_positive_direciton = 0
        self.worst_triple_axle_cnt_negative_direciton = 0
        self.worst_triple_axle_olhv_perc_negative_direciton = 0
        self.worst_triple_axle_tonperhv_negative_direciton = 0

        self.bridge_formula_cnt = round(
            (18000 + 2.1 * (df2.loc[df2['axle_spacing_number'] > 1].groupby('id')['axle_spacing_cm'].sum().mean())), 2) or 0
        self.bridge_formula_olhv_perc = 0
        self.bridge_formula_tonperhv = 0
        self.bridge_formula_cnt_positive_direciton = round((18000 + 2.1 * (df2.loc[(df2['axle_spacing_number'] > 1) & (
            df2['direction'] == 'P')].groupby('id')['axle_spacing_cm'].sum().mean())), 2) or 0
        self.bridge_formula_olhv_perc_positive_direciton = 0
        self.bridge_formula_tonperhv_positive_direciton = 0
        self.bridge_formula_cnt_negative_direciton = round((18000 + 2.1 * (df2.loc[(df2['axle_spacing_number'] > 1) & (
            df2['direction'] == 'P')].groupby('id')['axle_spacing_cm'].sum().mean())), 2) or 0
        self.bridge_formula_olhv_perc_negative_direciton = 0
        self.bridge_formula_tonperhv_negative_direciton = 0

        # TODO:
        self.gross_formula_cnt = 0
        self.gross_formula_olhv_perc = 0
        self.gross_formula_tonperhv = 0
        self.gross_formula_cnt_positive_direciton = 0
        self.gross_formula_olhv_perc_positive_direciton = 0
        self.gross_formula_tonperhv_positive_direciton = 0
        self.gross_formula_cnt_negative_direciton = 0
        self.gross_formula_olhv_perc_negative_direciton = 0
        self.gross_formula_tonperhv_negative_direciton = 0

        self.total_avg_cnt = df.loc[df['group'] == 'Heavy'].count()[0]
        self.total_avg_olhv_perc = 0
        self.total_avg_tonperhv = round(
            ((df3['wheel_mass']).sum()/1000)/df.loc[df['group'] == 'Heavy'].count()[0], 2)
        self.total_avg_cnt_positive_direciton = df.loc[(
            df['group'] == 'Heavy') & (df['direction'] == 'P')].count()[0]
        self.total_avg_olhv_perc_positive_direciton = 0
        self.total_avg_tonperhv_positive_direciton = round(
            ((df3.loc[df3['direction'] == 'P']['wheel_mass']).sum()/1000)/df.loc[df['group'] == 'Heavy'].count()[0], 2)
        self.total_avg_cnt_negative_direciton = df.loc[(
            df['group'] == 'Heavy') & (df['direction'] == 'N')].count()[0]
        self.total_avg_olhv_perc_negative_direciton = 0
        self.total_avg_tonperhv_negative_direciton = round(
            ((df3.loc[df3['direction'] == 'N']['wheel_mass']).sum()/1000)/df.loc[df['group'] == 'Heavy'].count()[0], 2)

        # FIXME:
        for i in range(4,18):
            try:
                setattr(self,f"total_vehicles_cls_{str(i)+'tot'}",df2.loc[(df2['vehicle_class_code_primary_scheme'] == i)].groupby('id').count().count()[0])
            except KeyError:
                setattr(self,f"total_vehicles_cls_{str(i)+'tot'}",0)
            
            try:
                setattr(self,f"weighed_vehicles_cls_{str(i)+'tot'}",df2.loc[(df2['vehicle_class_code_primary_scheme'] == i)].groupby('id').count().count()[0])
            except KeyError:
                setattr(self,f"weighed_vehicles_cls_{str(i)+'tot'}",0)

            try:
                setattr(self,f"perc_truck_dist_cls_{str(i)+'tot'}",(df2.loc[(df2['vehicle_class_code_primary_scheme'] == i)].groupby('id').count().count()[0]
                / df2.loc[(df2['vehicle_class_code_primary_scheme'] >= 4)].groupby('id').count().count()[0]))
            except KeyError:
                setattr(self,f"perc_truck_dist_cls_{str(i)+'tot'}",0)
            
            try:
                setattr(self,f"total_axles_cls_{str(i)+'tot'}",df2.loc[(df2['vehicle_class_code_primary_scheme'] == i)].groupby('id').count().sum()[0])
            except KeyError:
                setattr(self,f"total_axles_cls_{str(i)+'tot'}",0)
            
            try:
                setattr(self,f"axles_over9t_cls_{str(i)+'tot'}",df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['wheel_mass'] > 9000)].groupby('id').count().count()[0])
            except KeyError:
                setattr(self,f"axles_over9t_cls_{str(i)+'tot'}",0)
            
            try:
                setattr(self,f"total_mass_cls_{str(i)+'tot'}",df2.loc[(df2['vehicle_class_code_primary_scheme'] == i)].groupby('id')['wheel_mass'].sum()[0])
            except KeyError:
                setattr(self,f"total_mass_cls_{str(i)+'tot'}" ,0)                
            try:
                setattr(self,f"cnt_mass_over9t_cls_{str(i)+'tot'}",round(df2.loc[(df2['vehicle_class_code_primary_scheme'] == i),'wheel_mass'].sum()/1000))
            except KeyError:
                setattr(self,f"cnt_mass_over9t_cls_{str(i)+'tot'}",0)
            
            try:
                setattr(self,f"eal_pervehicle_cls_{str(i)+'tot'}",(round(df2.loc[(df2['vehicle_class_code_primary_scheme'] == i),'wheel_mass'].sum()/1000)/
                df2.loc[(df2['vehicle_class_code_primary_scheme'] == i)].groupby('id').count().sum()[0]))
            except KeyError:
                setattr(self,f"eal_pervehicle_cls_{str(i)+'tot'}",0)
            
            try:
                setattr(self,f"total_eal_cls_{str(i)+'tot'}",((round(df2.loc[(df2['vehicle_class_code_primary_scheme'] == i),'wheel_mass'].sum()/1000)/
                df2.loc[(df2['vehicle_class_code_primary_scheme'] == i)].groupby('id').count().sum()[0])
                *df2.loc[(df2['vehicle_class_code_primary_scheme'] == i)].groupby('id').count().count()[0]))
            except KeyError:
                setattr(self,f"total_eal_cls_{str(i)+'tot'}",0)
            
            try:
                setattr(self,f"total_eal_over9t_cls_{str(i)+'tot'}",0)
            except KeyError:
                setattr(self,f"total_eal_over9t_cls_{str(i)+'tot'}",0)

            for dir in ['P','N']:
                try:
                    setattr(self,f"total_vehicles_cls_{str(i)+dir.lower()}",df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir)].groupby('id').count().count()[0])
                except KeyError:
                    setattr(self,f"total_vehicles_cls_{str(i)+dir.lower()}",0)
                
                try:
                    setattr(self,f"weighed_vehicles_cls_{str(i)+dir.lower()}",df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir)].groupby('id').count().count()[0])
                except KeyError:
                    setattr(self,f"weighed_vehicles_cls_{str(i)+dir.lower()}",0)

                try:
                    setattr(self,f"perc_truck_dist_cls_{str(i)+dir.lower()}",(df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir)].groupby('id').count().count()[0]
                    / df2.loc[(df2['vehicle_class_code_primary_scheme'] >= 4) & (df2['direction'] == dir)].groupby('id').count().count()[0]))
                except KeyError:
                    setattr(self,f"perc_truck_dist_cls_{str(i)+dir.lower()}",0)
                
                try:
                    setattr(self,f"total_axles_cls_{str(i)+dir.lower()}",df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir)].groupby('id').count().sum()[0])
                except KeyError:
                    setattr(self,f"total_axles_cls_{str(i)+dir.lower()}",0)
                
                try:
                    setattr(self,f"axles_over9t_cls_{str(i)+dir.lower()}",df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir) & (df2['wheel_mass'] > 9000)].groupby('id').count().count()[0])
                except KeyError:
                    setattr(self,f"axles_over9t_cls_{str(i)+dir.lower()}",0)
                
                try:
                    setattr(self,f"total_mass_cls_{str(i)+dir.lower()}",df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir)].groupby('id')['wheel_mass'].sum()[0])
                except KeyError:
                    setattr(self,f"total_mass_cls_{str(i)+dir.lower()}" ,0)                
                try:
                    setattr(self,f"cnt_mass_over9t_cls_{str(i)+dir.lower()}",round(df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir),'wheel_mass'].sum()/1000))
                except KeyError:
                    setattr(self,f"cnt_mass_over9t_cls_{str(i)+dir.lower()}",0)
                
                try:
                    setattr(self,f"eal_pervehicle_cls_{str(i)+dir.lower()}",(round(df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir),'wheel_mass'].sum()/1000)/
                    df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir)].groupby('id').count().sum()[0]))
                except KeyError:
                    setattr(self,f"eal_pervehicle_cls_{str(i)+dir.lower()}",0)
                
                try:
                    setattr(self,f"total_eal_cls_{str(i)+dir.lower()}",((round(df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir),'wheel_mass'].sum()/1000)/
                    df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir)].groupby('id').count().sum()[0])
                    *df2.loc[(df2['vehicle_class_code_primary_scheme'] == i) & (df2['direction'] == dir)].groupby('id').count().count()[0]))
                except KeyError:
                    setattr(self,f"total_eal_cls_{str(i)+dir.lower()}",0)
                
                try:
                    setattr(self,f"total_eal_over9t_cls_{str(i)+dir.lower()}",0)
                except KeyError:
                    setattr(self,f"total_eal_over9t_cls_{str(i)+dir.lower()}",0)

        # return pd.DataFrame({
        #         "header_id": self.header_id ,
        #         "egrl_percent": self.egrl_percent ,
        #         "egrw_percent": self.egrw_percent ,
        #         "mean_equivalent_axle_mass": self.mean_equivalent_axle_mass ,
        #         "mean_equivalent_axle_mass_positive_direction": self.mean_equivalent_axle_mass_positive_direction ,
        #         "mean_equivalent_axle_mass_negative_direction": self.mean_equivalent_axle_mass_negative_direction ,
        #         "mean_axle_spacing": self.mean_axle_spacing ,
        #         "mean_axle_spacing_positive_direction": self.mean_axle_spacing_positive_direction ,
        #         "mean_axle_spacing_negative_direction": self.mean_axle_spacing_negative_direction ,
        #         "e80_per_axle": self.e80_per_axle ,
        #         "e80_per_axle_positive_direction": self.e80_per_axle_positive_direction ,
        #         "e80_per_axle_negative_direction": self.e80_per_axle_negative_direction ,
        #         "olhv": self.olhv ,
        #         "olhv_positive_direction": self.olhv_positive_direction ,
        #         "olhv_negative_direction": self.olhv_negative_direction ,
        #         "olhv_percent": self.olhv_percent ,
        #         "olhv_percent_positive_direction": self.olhv_percent_positive_direction ,
        #         "olhv_percent_negative_direction": self.olhv_percent_negative_direction ,
        #         "tonnage_generated": self.tonnage_generated ,
        #         "tonnage_generated_positive_direction": self.tonnage_generated_positive_direction ,
        #         "tonnage_generated_negative_direction": self.tonnage_generated_negative_direction ,
        #         "olton": self.olton ,
        #         "olton_positive_direction": self.olton_positive_direction ,
        #         "olton_negative_direction": self.olton_negative_direction ,
        #         "olton_percent": self.olton_percent ,
        #         "olton_percent_positive_direction": self.olton_percent_positive_direction ,
        #         "olton_percent_negative_direction": self.olton_percent_negative_direction ,
        #         "ole80": self.ole80 ,
        #         "ole80_positive_direction": self.ole80_positive_direction ,
        #         "ole80_negative_direction": self.ole80_negative_direction ,
        #         "ole80_percent": self.ole80_percent ,
        #         "ole80_percent_positive_direction": self.ole80_percent_positive_direction ,
        #         "ole80_percent_negative_direction": self.ole80_percent_negative_direction ,
        #         "xe80": self.xe80 ,
        #         "xe80_positive_direction": self.xe80_positive_direction ,
        #         "xe80_negative_direction": self.xe80_negative_direction ,
        #         "xe80_percent": self.xe80_percent ,
        #         "xe80_percent_positive_direction": self.xe80_percent_positive_direction ,
        #         "xe80_percent_negative_direction": self.xe80_percent_negative_direction ,
        #         "e80_per_day": self.e80_per_day ,
        #         "e80_per_day_positive_direction": self.e80_per_day_positive_direction ,
        #         "e80_per_day_negative_direction": self.e80_per_day_negative_direction ,
        #         "e80_per_heavy_vehicle": self.e80_per_heavy_vehicle ,
        #         "e80_per_heavy_vehicle_positive_direction": self.e80_per_heavy_vehicle_positive_direction ,
        #         "e80_per_heavy_vehicle_negative_direction": self.e80_per_heavy_vehicle_negative_direction ,
        #         "worst_steering_single_axle_cnt": self.worst_steering_single_axle_cnt ,
        #         "worst_steering_single_axle_olhv_perc": self.worst_steering_single_axle_olhv_perc ,
        #         "worst_steering_single_axle_tonperhv": self.worst_steering_single_axle_tonperhv ,
        #         "worst_steering_double_axle_cnt": self.worst_steering_double_axle_cnt ,
        #         "worst_steering_double_axle_olhv_perc": self.worst_steering_double_axle_olhv_perc ,
        #         "worst_steering_double_axle_tonperhv": self.worst_steering_double_axle_tonperhv ,
        #         "worst_non_steering_single_axle_cnt": self.worst_non_steering_single_axle_cnt ,
        #         "worst_non_steering_single_axle_olhv_perc": self.worst_non_steering_single_axle_olhv_perc ,
        #         "worst_non_steering_single_axle_tonperhv": self.worst_non_steering_single_axle_tonperhv ,
        #         "worst_non_steering_double_axle_cnt": self.worst_non_steering_double_axle_cnt ,
        #         "worst_non_steering_double_axle_olhv_perc": self.worst_non_steering_double_axle_olhv_perc ,
        #         "worst_non_steering_double_axle_tonperhv": self.worst_non_steering_double_axle_tonperhv ,
        #         "worst_triple_axle_cnt": self.worst_triple_axle_cnt ,
        #         "worst_triple_axle_olhv_perc": self.worst_triple_axle_olhv_perc ,
        #         "worst_triple_axle_tonperhv": self.worst_triple_axle_tonperhv ,
        #         "bridge_formula_cnt": self.bridge_formula_cnt ,
        #         "bridge_formula_olhv_perc": self.bridge_formula_olhv_perc ,
        #         "bridge_formula_tonperhv": self.bridge_formula_tonperhv ,
        #         "gross_formula_cnt": self.gross_formula_cnt ,
        #         "gross_formula_olhv_perc": self.gross_formula_olhv_perc ,
        #         "gross_formula_tonperhv": self.gross_formula_tonperhv ,
        #         "total_avg_cnt": self.total_avg_cnt ,
        #         "total_avg_olhv_perc": self.total_avg_olhv_perc ,
        #         "total_avg_tonperhv": self.total_avg_tonperhv ,
        #         "worst_steering_single_axle_cnt_positive_direciton": self.worst_steering_single_axle_cnt_positive_direciton ,
        #         "worst_steering_single_axle_olhv_perc_positive_direciton": self.worst_steering_single_axle_olhv_perc_positive_direciton ,
        #         "worst_steering_single_axle_tonperhv_positive_direciton": self.worst_steering_single_axle_tonperhv_positive_direciton ,
        #         "worst_steering_double_axle_cnt_positive_direciton": self.worst_steering_double_axle_cnt_positive_direciton ,
        #         "worst_steering_double_axle_olhv_perc_positive_direciton": self.worst_steering_double_axle_olhv_perc_positive_direciton ,
        #         "worst_steering_double_axle_tonperhv_positive_direciton": self.worst_steering_double_axle_tonperhv_positive_direciton ,
        #         "worst_non_steering_single_axle_cnt_positive_direciton": self.worst_non_steering_single_axle_cnt_positive_direciton ,
        #         "worst_non_steering_single_axle_olhv_perc_positive_direciton": self.worst_non_steering_single_axle_olhv_perc_positive_direciton ,
        #         "worst_non_steering_single_axle_tonperhv_positive_direciton": self.worst_non_steering_single_axle_tonperhv_positive_direciton ,
        #         "worst_non_steering_double_axle_cnt_positive_direciton": self.worst_non_steering_double_axle_cnt_positive_direciton ,
        #         "worst_non_steering_double_axle_olhv_perc_positive_direciton": self.worst_non_steering_double_axle_olhv_perc_positive_direciton ,
        #         "worst_non_steering_double_axle_tonperhv_positive_direciton": self.worst_non_steering_double_axle_tonperhv_positive_direciton ,
        #         "worst_triple_axle_cnt_positive_direciton": self.worst_triple_axle_cnt_positive_direciton ,
        #         "worst_triple_axle_olhv_perc_positive_direciton": self.worst_triple_axle_olhv_perc_positive_direciton ,
        #         "worst_triple_axle_tonperhv_positive_direciton": self.worst_triple_axle_tonperhv_positive_direciton ,
        #         "bridge_formula_cnt_positive_direciton": self.bridge_formula_cnt_positive_direciton ,
        #         "bridge_formula_olhv_perc_positive_direciton": self.bridge_formula_olhv_perc_positive_direciton ,
        #         "bridge_formula_tonperhv_positive_direciton": self.bridge_formula_tonperhv_positive_direciton ,
        #         "gross_formula_cnt_positive_direciton": self.gross_formula_cnt_positive_direciton ,
        #         "gross_formula_olhv_perc_positive_direciton": self.gross_formula_olhv_perc_positive_direciton ,
        #         "gross_formula_tonperhv_positive_direciton": self.gross_formula_tonperhv_positive_direciton ,
        #         "total_avg_cnt_positive_direciton": self.total_avg_cnt_positive_direciton ,
        #         "total_avg_olhv_perc_positive_direciton": self.total_avg_olhv_perc_positive_direciton ,
        #         "total_avg_tonperhv_positive_direciton": self.total_avg_tonperhv_positive_direciton ,
        #         "worst_steering_single_axle_cnt_negative_direciton": self.worst_steering_single_axle_cnt_negative_direciton ,
        #         "worst_steering_single_axle_olhv_perc_negative_direciton": self.worst_steering_single_axle_olhv_perc_negative_direciton ,
        #         "worst_steering_single_axle_tonperhv_negative_direciton": self.worst_steering_single_axle_tonperhv_negative_direciton ,
        #         "worst_steering_double_axle_cnt_negative_direciton": self.worst_steering_double_axle_cnt_negative_direciton ,
        #         "worst_steering_double_axle_olhv_perc_negative_direciton": self.worst_steering_double_axle_olhv_perc_negative_direciton ,
        #         "worst_steering_double_axle_tonperhv_negative_direciton": self.worst_steering_double_axle_tonperhv_negative_direciton ,
        #         "worst_non_steering_single_axle_cnt_negative_direciton": self.worst_non_steering_single_axle_cnt_negative_direciton ,
        #         "worst_non_steering_single_axle_olhv_perc_negative_direciton": self.worst_non_steering_single_axle_olhv_perc_negative_direciton ,
        #         "worst_non_steering_single_axle_tonperhv_negative_direciton": self.worst_non_steering_single_axle_tonperhv_negative_direciton ,
        #         "worst_non_steering_double_axle_cnt_negative_direciton": self.worst_non_steering_double_axle_cnt_negative_direciton ,
        #         "worst_non_steering_double_axle_olhv_perc_negative_direciton": self.worst_non_steering_double_axle_olhv_perc_negative_direciton ,
        #         "worst_non_steering_double_axle_tonperhv_negative_direciton": self.worst_non_steering_double_axle_tonperhv_negative_direciton ,
        #         "worst_triple_axle_cnt_negative_direciton": self.worst_triple_axle_cnt_negative_direciton ,
        #         "worst_triple_axle_olhv_perc_negative_direciton": self.worst_triple_axle_olhv_perc_negative_direciton ,
        #         "worst_triple_axle_tonperhv_negative_direciton": self.worst_triple_axle_tonperhv_negative_direciton ,
        #         "bridge_formula_cnt_negative_direciton": self.bridge_formula_cnt_negative_direciton ,
        #         "bridge_formula_olhv_perc_negative_direciton": self.bridge_formula_olhv_perc_negative_direciton ,
        #         "bridge_formula_tonperhv_negative_direciton": self.bridge_formula_tonperhv_negative_direciton ,
        #         "gross_formula_cnt_negative_direciton": self.gross_formula_cnt_negative_direciton ,
        #         "gross_formula_olhv_perc_negative_direciton": self.gross_formula_olhv_perc_negative_direciton ,
        #         "gross_formula_tonperhv_negative_direciton": self.gross_formula_tonperhv_negative_direciton ,
        #         "total_avg_cnt_negative_direciton": self.total_avg_cnt_negative_direciton ,
        #         "total_avg_olhv_perc_negative_direciton": self.total_avg_olhv_perc_negative_direciton ,
        #         "total_avg_tonperhv_negative_direciton": self.total_avg_tonperhv_negative_direciton ,
        #         "egrl_percent_positive_direction": self.egrl_percent_positive_direction ,
        #         "egrl_percent_negative_direction": self.egrl_percent_negative_direction ,
        #         "egrw_percent_positive_direction": self.egrw_percent_positive_direction ,
        #         "egrw_percent_negative_direction": self.egrw_percent_negative_direction ,
        #         "num_weighed": self.num_weighed ,
        #         "num_weighed_positive_direction": self.num_weighed_positive_direction ,
        #         "num_weighed_negative_direction": self.num_weighed_negative_direction
        #     })

    def summary_header_calcs(self, header: pd.DataFrame, data: pd.DataFrame, type: int) -> pd.DataFrame:
        try:
            speed_limit_qry = f"select max_speed from trafc.countstation where tcname = '{self.site_id}' ;"
            speed_limit = pd.read_sql_query(
                speed_limit_qry, config.ENGINE).reset_index(drop=True)
            try:
                speed_limit = speed_limit['max_speed'].iloc[0]
            except IndexError:
                speed_limit = 60
            if data.empty:
                pass
            else:
                data = data.fillna(0, axis=0)
                if type == 10:
                    try:
                        header['total_light_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme'] <= 1) & (
                            data['direction'] == 'N')].count()[0].round().astype(int)
                    except (ValueError, IndexError):
                        header['total_light_negative_direction'] = 0
                    try:
                        header['total_light_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme'] <= 1) & (
                            data['direction'] == 'P')].count()[0].round().astype(int)
                    except (ValueError, IndexError):
                        header['total_light_positive_direction'] = 0
                    header['total_light_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme'] <= 1].count()[
                        0].round().astype(int)
                    try:
                        header['total_heavy_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme'] > 1) & (
                            data['direction'] == 'N')].count()[0].round().astype(int)
                    except (ValueError, IndexError):
                        header['total_heavy_negative_direction'] = 0
                    try:
                        header['total_heavy_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme'] > 1) & (
                            data['direction'] == 'P')].count()[0].round().astype(int)
                    except (ValueError, IndexError):
                        header['total_heavy_positive_direction'] = 0
                    header['total_heavy_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme'] > 1].count()[
                        0].round().astype(int)
                    try:
                        header['total_short_heavy_negative_direction'] = data.loc[(
                            data['vehicle_class_code_secondary_scheme'] == 2) & (data['direction'] == 'N')].count()[0].round().astype(int)
                    except (ValueError, IndexError):
                        header['total_short_heavy_negative_direction'] = 0
                    try:
                        header['total_short_heavy_positive_direction'] = data.loc[(
                            data['vehicle_class_code_secondary_scheme'] == 2) & (data['direction'] == 'P')].count()[0].round().astype(int)
                    except (ValueError, IndexError):
                        header['total_short_heavy_positive_direction'] = 0
                    header['total_short_heavy_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme'] == 2].count()[
                        0].round().astype(int)
                    try:
                        header['total_medium_heavy_negative_direction'] = data.loc[(
                            data['vehicle_class_code_secondary_scheme'] == 3) & (data['direction'] == 'N')].count()[0].round().astype(int)
                    except (ValueError, IndexError):
                        header['total_medium_heavy_negative_direction'] = 0
                    try:
                        header['total_medium_heavy_positive_direction'] = data.loc[(
                            data['vehicle_class_code_secondary_scheme'] == 3) & (data['direction'] == 'P')].count()[0].round().astype(int)
                    except (ValueError, IndexError):
                        header['total_medium_heavy_positive_direction'] = 0
                    header['total_medium_heavy_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme'] == 3].count()[
                        0].round().astype(int)
                    try:
                        header['total_long_heavy_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme'] == 4) & (
                            data['direction'] == 'N')].count()[0].round().astype(int)
                    except (ValueError, IndexError):
                        header['total_long_heavy_negative_direction'] = 0
                    try:
                        header['total_long_heavy_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme'] == 4) & (
                            data['direction'] == 'P')].count()[0].round().astype(int)
                    except (ValueError, IndexError):
                        header['total_long_heavy_positive_direction'] = 0
                    header['total_long_heavy_vehicles'] = data.loc[data['vehicle_class_code_secondary_scheme'] == 4].count()[
                        0].round().astype(int)
                    try:
                        header['total_vehicles_negative_direction'] = data.loc[data['direction'] == 'N'].count()[
                            0].round().astype(int)
                    except (ValueError, IndexError):
                        header['total_vehicles_negative_direction'] = 0
                    try:
                        header['total_vehicles_positive_direction'] = data.loc[data['direction'] == 'P'].count()[
                            0].round().astype(int)
                    except (ValueError, IndexError):
                        header['total_vehicles_positive_direction'] = 0
                    header['total_vehicles'] = data.count()[
                        0].round().astype(int)
                    try:
                        header['average_speed_negative_direction'] = data.loc[data['direction']
                                                                              == 'N']['vehicle_speed'].mean().round(4)
                    except (ValueError, IndexError):
                        header['average_speed_negative_direction'] = 0
                    try:
                        header['average_speed_positive_direction'] = data.loc[data['direction']
                                                                              == 'P']['vehicle_speed'].mean().round(4)
                    except (ValueError, IndexError):
                        header['average_speed_positive_direction'] = 0
                    header['average_speed'] = data['vehicle_speed'].mean().round(4)
                    try:
                        header['average_speed_light_vehicles_negative_direction'] = data['vehicle_speed'].loc[(
                            data['vehicle_class_code_secondary_scheme'] <= 1) & (data['direction'] == 'N')].mean().round(4)
                    except (ValueError, IndexError):
                        header['average_speed_light_vehicles_negative_direction'] = 0
                    try:
                        header['average_speed_light_vehicles_positive_direction'] = data['vehicle_speed'].loc[(
                            data['vehicle_class_code_secondary_scheme'] <= 1) & (data['direction'] == 'P')].mean().round(4)
                    except (ValueError, IndexError):
                        header['average_speed_light_vehicles_positive_direction'] = 0
                    header['average_speed_light_vehicles'] = data['vehicle_speed'].loc[
                        data['vehicle_class_code_secondary_scheme'] <= 1].mean().round(4)
                    try:
                        header['average_speed_heavy_vehicles_negative_direction'] = data['vehicle_speed'].loc[(
                            data['vehicle_class_code_secondary_scheme'] > 1) & (data['direction'] == 'N')].mean().round(4)
                    except (ValueError, IndexError):
                        header['average_speed_heavy_vehicles_negative_direction'] = 0
                    try:
                        header['average_speed_heavy_vehicles_positive_direction'] = data['vehicle_speed'].loc[(
                            data['vehicle_class_code_secondary_scheme'] > 1) & (data['direction'] == 'P')].mean().round(4)
                    except (ValueError, IndexError):
                        header['average_speed_heavy_vehicles_positive_direction'] = 0
                    header['average_speed_heavy_vehicles'] = data['vehicle_speed'].loc[data['vehicle_class_code_secondary_scheme'] > 1].mean(
                    ).round(4)
                    try:
                        header['truck_split_negative_direction'] = {str((((data.loc[(data['vehicle_class_code_secondary_scheme'] == 2) & (data['direction'] == 'N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme'] > 1) & (data['direction'] == 'N')].count())[0])*100).round().astype(int)) + ":" + str((((data.loc[(data['vehicle_class_code_secondary_scheme'] == 3) & (data['direction'] == 'N')].count(
                        )/data.loc[(data['vehicle_class_code_secondary_scheme'] > 1) & (data['direction'] == 'N')].count())[0])*100).round().astype(int)) + ":" + str((((data.loc[(data['vehicle_class_code_secondary_scheme'] == 4) & (data['direction'] == 'N')].count()/data.loc[(data['vehicle_class_code_secondary_scheme'] > 1) & (data['direction'] == 'N')].count())[0])*100).round().astype(int))}
                    except (ValueError, IndexError):
                        header['truck_split_negative_direction'] = 0
                    try:
                        header['truck_split_positive_direction'] = {str((((data.loc[(data['vehicle_class_code_secondary_scheme'] == 2) & (data['direction'] == 'P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme'] > 1) & (data['direction'] == 'P')].count())[0])*100).round().astype(int)) + ":" + str((((data.loc[(data['vehicle_class_code_secondary_scheme'] == 3) & (data['direction'] == 'P')].count(
                        )/data.loc[(data['vehicle_class_code_secondary_scheme'] > 1) & (data['direction'] == 'P')].count())[0])*100).round().astype(int)) + ":" + str((((data.loc[(data['vehicle_class_code_secondary_scheme'] == 4) & (data['direction'] == 'P')].count()/data.loc[(data['vehicle_class_code_secondary_scheme'] > 1) & (data['direction'] == 'P')].count())[0])*100).round().astype(int))}
                    except (ValueError, IndexError):
                        header['truck_split_positive_direction'] = 0
                    header['truck_split_total'] = {str((((data.loc[data['vehicle_class_code_secondary_scheme'] == 2].count()/data.loc[data['vehicle_class_code_secondary_scheme'] > 1].count())[0])*100).round().astype(int)) + ":" + str((((data.loc[data['vehicle_class_code_secondary_scheme'] == 3].count(
                    )/data.loc[data['vehicle_class_code_secondary_scheme'] > 1].count())[0])*100).round().astype(int)) + ":" + str((((data.loc[data['vehicle_class_code_secondary_scheme'] == 4].count()/data.loc[data['vehicle_class_code_secondary_scheme'] > 1].count())[0])*100).round().astype(int))}
                    try:
                        header['estimated_axles_per_truck_negative_direction'] = ((data.loc[(data['vehicle_class_code_secondary_scheme'] == 2) & (data['direction'] == 'N')].count()[0]*2+data.loc[(data['vehicle_class_code_secondary_scheme'] == 3) & (data['direction'] == 'N')].count()[0]*5+data.loc[(data['vehicle_class_code_secondary_scheme'] == 4) & (data['direction'] == 'N')].count()[
                            0]*7)/(data.loc[(data['vehicle_class_code_secondary_scheme'] == 2) & (data['direction'] == 'N')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme'] == 3) & (data['direction'] == 'N')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme'] == 4) & (data['direction'] == 'N')].count()[0])).round(4)
                    except (ValueError, IndexError):
                        header['estimated_axles_per_truck_negative_direction'] = 0
                    try:
                        header['estimated_axles_per_truck_positive_direction'] = ((data.loc[(data['vehicle_class_code_secondary_scheme'] == 2) & (data['direction'] == 'P')].count()[0]*2+data.loc[(data['vehicle_class_code_secondary_scheme'] == 3) & (data['direction'] == 'P')].count()[0]*5+data.loc[(data['vehicle_class_code_secondary_scheme'] == 4) & (data['direction'] == 'P')].count()[
                            0]*7)/(data.loc[(data['vehicle_class_code_secondary_scheme'] == 2) & (data['direction'] == 'P')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme'] == 3) & (data['direction'] == 'P')].count()[0]+data.loc[(data['vehicle_class_code_secondary_scheme'] == 4) & (data['direction'] == 'P')].count()[0])).round(4)
                    except (ValueError, IndexError):
                        header['estimated_axles_per_truck_positive_direction'] = 0
                    header['estimated_axles_per_truck_total'] = ((data.loc[data['vehicle_class_code_secondary_scheme'] == 2].count()[0]*2+data.loc[data['vehicle_class_code_secondary_scheme'] == 3].count()[0]*5+data.loc[data['vehicle_class_code_secondary_scheme'] == 4].count()[
                        0]*7)/(data.loc[data['vehicle_class_code_secondary_scheme'] == 2].count()[0]+data.loc[data['vehicle_class_code_secondary_scheme'] == 3].count()[0]+data.loc[data['vehicle_class_code_secondary_scheme'] == 4].count()[0])).round(4)
                    try:
                        header['percentage_speeding_positive_direction'] = ((data.loc[(data['vehicle_speed'] > speed_limit) & (
                            data['direction'] == 'P')].count()[0]/data.loc[data['direction' == 'P']].count()[0])*100).round(4)
                    except (ValueError, IndexError):
                        header['percentage_speeding_positive_direction'] = 0
                    try:
                        header['percentage_speeding_negative_direction'] = ((data.loc[(data['vehicle_speed'] > speed_limit) & (
                            data['direction'] == 'N')].count()[0]/data.loc[data['direction' == 'N']].count()[0])*100).round(4)
                    except (ValueError, IndexError):
                        header['percentage_speeding_negative_direction'] = 0
                    header['percentage_speeding_total'] = (
                        (data.loc[data['vehicle_speed'] > speed_limit].count()[0]/data.count()[0])*100).round(4)
                    try:
                        header['vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire'] = data.loc[(
                            data['vehicle_following_code'] == 2) & data['direction'] == 'N'].count()[0]
                    except (ValueError, IndexError):
                        header['vehicles_with_rear_to_rear_headway_less_than_2sec_negative_dire'] = 0
                    try:
                        header['vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire'] = data.loc[(
                            data['vehicle_following_code'] == 2) & data['direction'] == 'P'].count()[0]
                    except (ValueError, IndexError):
                        header['vehicles_with_rear_to_rear_headway_less_than_2sec_positive_dire'] = 0
                    header['vehicles_with_rear_to_rear_headway_less_than_2sec_total'] = data.loc[data['vehicle_following_code'] == 2].count()[
                        0]
                    try:
                        header['estimated_e80_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme'] == 2) & (data['direction'] == 'N')].count()[0]*0.6+data.loc[(
                            data['vehicle_class_code_secondary_scheme'] == 3) & (data['direction'] == 'N')].count()[0]*2.5+data.loc[(data['vehicle_class_code_secondary_scheme'] == 4) & (data['direction'] == 'N')].count()[0]*2.1
                    except (ValueError, IndexError):
                        header['estimated_e80_negative_direction'] = 0
                    try:
                        header['estimated_e80_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme'] == 2) & (data['direction'] == 'P')].count()[0]*0.6+data.loc[(
                            data['vehicle_class_code_secondary_scheme'] == 3) & (data['direction'] == 'P')].count()[0]*2.5+data.loc[(data['vehicle_class_code_secondary_scheme'] == 4) & (data['direction'] == 'P')].count()[0]*2.1
                    except (ValueError, IndexError):
                        header['estimated_e80_positive_direction'] = 0
                    header['estimated_e80_on_road'] = data.loc[data['vehicle_class_code_secondary_scheme'] == 2].count(
                    )[0]*0.6+data.loc[data['vehicle_class_code_secondary_scheme'] == 3].count()[0]*2.5+data.loc[data['vehicle_class_code_secondary_scheme'] == 4].count()[0]*2.1
                    try:
                        header['adt_negative_direction'] = data.loc[data['direction'] == 'N'].groupby(
                            pd.Grouper(key='start_datetime', freq='D')).count().mean()[0].round().astype(int)
                    except (ValueError, IndexError):
                        header['adt_negative_direction'] = 0
                    try:
                        header['adt_positive_direction'] = data.loc[data['direction'] == 'P'].groupby(
                            pd.Grouper(key='start_datetime', freq='D')).count().mean()[0].round().astype(int)
                    except (ValueError, IndexError):
                        header['adt_positive_direction'] = 0
                    header['adt_total'] = data.groupby(pd.Grouper(
                        key='start_datetime', freq='D')).count().mean()[0].round().astype(int)
                    try:
                        header['adtt_negative_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme'] > 1) & (
                            data['direction'] == 'N')].groupby(pd.Grouper(key='start_datetime', freq='D')).count().mean()[0].round().astype(int)
                    except (ValueError, IndexError):
                        header['adtt_negative_direction'] = 0
                    try:
                        header['adtt_positive_direction'] = data.loc[(data['vehicle_class_code_secondary_scheme'] > 1) & (
                            data['direction'] == 'P')].groupby(pd.Grouper(key='start_datetime', freq='D')).count().mean()[0].round().astype(int)
                    except (ValueError, IndexError):
                        header['adtt_positive_direction'] = 0
                    header['adtt_total'] = data.loc[data['vehicle_class_code_secondary_scheme'] > 1].groupby(
                        pd.Grouper(key='start_datetime', freq='D')).count().mean()[0].round().astype(int)
                    try:
                        header['highest_volume_per_hour_negative_direction'] = data.loc[data['direction'] == 'N'].groupby(
                            pd.Grouper(key='start_datetime', freq='H')).count().max()[0]
                    except (ValueError, IndexError):
                        header['highest_volume_per_hour_negative_direction'] = 0
                    try:
                        header['highest_volume_per_hour_positive_direction'] = data.loc[data['direction'] == 'P'].groupby(
                            pd.Grouper(key='start_datetime', freq='H')).count().max()[0]
                    except (ValueError, IndexError):
                        header['highest_volume_per_hour_positive_direction'] = 0
                    header['highest_volume_per_hour_total'] = data.groupby(
                        pd.Grouper(key='start_datetime', freq='H')).count().max()[0]
                    try:
                        header["15th_highest_volume_per_hour_negative_direction"] = round(data.loc[data['direction'] == 'N'].groupby(
                            pd.Grouper(key='start_datetime', freq='D')).count().quantile(0.15)[0].round().astype(int))
                    except (ValueError, IndexError):
                        header["15th_highest_volume_per_hour_negative_direction"] = 0
                    try:
                        header["15th_highest_volume_per_hour_positive_direction"] = round(data.loc[data['direction'] == 'P'].groupby(
                            pd.Grouper(key='start_datetime', freq='D')).count().quantile(0.15)[0].round().astype(int))
                    except (ValueError, IndexError):
                        header["15th_highest_volume_per_hour_positive_direction"] = 0
                    header["15th_highest_volume_per_hour_total"] = data.groupby(pd.Grouper(
                        key='start_datetime', freq='D')).count().quantile(0.15)[0].round().astype(int)
                    try:
                        header["30th_highest_volume_per_hour_negative_direction"] = round(data.loc[data['direction'] == 'N'].groupby(
                            pd.Grouper(key='start_datetime', freq='D')).count().quantile(0.30)[0].round().astype(int))
                    except (ValueError, IndexError):
                        header["30th_highest_volume_per_hour_negative_direction"] = 0
                    try:
                        header["30th_highest_volume_per_hour_positive_direction"] = round(data.loc[data['direction'] == 'P'].groupby(
                            pd.Grouper(key='start_datetime', freq='D')).count().quantile(0.30)[0].round().astype(int))
                    except (ValueError, IndexError):
                        header["30th_highest_volume_per_hour_positive_direction"] = 0
                    header["30th_highest_volume_per_hour_total"] = data.groupby(pd.Grouper(
                        key='start_datetime', freq='D')).count().quantile(0.30)[0].round().astype(int)
                    try:
                        header["15th_percentile_speed_negative_direction"] = data.loc[data['direction']
                                                                                      == 'N']['vehicle_speed'].quantile(0.15).round(4)
                    except (ValueError, IndexError):
                        header["15th_percentile_speed_negative_direction"] = 0
                    try:
                        header["15th_percentile_speed_positive_direction"] = data.loc[data['direction']
                                                                                      == 'P']['vehicle_speed'].quantile(0.15).round(4)
                    except (ValueError, IndexError):
                        header["15th_percentile_speed_positive_direction"] = 0
                    header["15th_percentile_speed_total"] = data['vehicle_speed'].quantile(
                        0.15).round(4)
                    try:
                        header["85th_percentile_speed_negative_direction"] = data.loc[data['direction']
                                                                                      == 'N']['vehicle_speed'].quantile(0.85).round(4)
                    except (ValueError, IndexError):
                        header["85th_percentile_speed_negative_direction"] = 0
                    try:
                        header["85th_percentile_speed_positive_direction"] = data.loc[data['direction']
                                                                                      == 'P']['vehicle_speed'].quantile(0.85).round(4)
                    except (ValueError, IndexError):
                        header["85th_percentile_speed_positive_direction"] = 0
                    header["85th_percentile_speed_total"] = data['vehicle_speed'].quantile(
                        0.85).round(4)
                    header['avg_weekday_traffic'] = data.groupby(pd.Grouper(
                        key='start_datetime', freq='B')).count().mean()[0].round().astype(int)
                    header['number_of_days_counted'] = data.groupby(
                        [data['start_datetime'].dt.to_period('D')]).count().count()[0]
                    header['duration_hours'] = data.groupby(
                        [data['start_datetime'].dt.to_period('H')]).count().count()[0]

                    return header
        except IndexError:
            return header

    def wim_header_upsert(self, header_id) -> str:
        UPSERT_STRING = F"""INSERT INTO trafc.electronic_count_header_hswim (
            header_id,
            egrl_percent,
            egrw_percent,
            mean_equivalent_axle_mass,
            mean_equivalent_axle_mass_positive_direction,
            mean_equivalent_axle_mass_negative_direction,
            mean_axle_spacing,
            mean_axle_spacing_positive_direction,
            mean_axle_spacing_negative_direction,
            e80_per_axle,
            e80_per_axle_positive_direction,
            e80_per_axle_negative_direction,
            olhv,
            olhv_positive_direction,
            olhv_negative_direction,
            olhv_percent,
            olhv_percent_positive_direction,
            olhv_percent_negative_direction,
            tonnage_generated,
            tonnage_generated_positive_direction,
            tonnage_generated_negative_direction,
            olton,
            olton_positive_direction,
            olton_negative_direction,
            olton_percent,
            olton_percent_positive_direction,
            olton_percent_negative_direction,
            ole80,
            ole80_positive_direction,
            ole80_negative_direction,
            ole80_percent,
            ole80_percent_positive_direction,
            ole80_percent_negative_direction,
            xe80,
            xe80_positive_direction,
            xe80_negative_direction,
            xe80_percent,
            xe80_percent_positive_direction,
            xe80_percent_negative_direction,
            e80_per_day,
            e80_per_day_positive_direction,
            e80_per_day_negative_direction,
            e80_per_heavy_vehicle,
            e80_per_heavy_vehicle_positive_direction,
            e80_per_heavy_vehicle_negative_direction,
            worst_steering_single_axle_cnt,
            worst_steering_single_axle_olhv_perc,
            worst_steering_single_axle_tonperhv,
            worst_steering_double_axle_cnt,
            worst_steering_double_axle_olhv_perc,
            worst_steering_double_axle_tonperhv,
            worst_non_steering_single_axle_cnt,
            worst_non_steering_single_axle_olhv_perc,
            worst_non_steering_single_axle_tonperhv,
            worst_non_steering_double_axle_cnt,
            worst_non_steering_double_axle_olhv_perc,
            worst_non_steering_double_axle_tonperhv,
            worst_triple_axle_cnt,
            worst_triple_axle_olhv_perc,
            worst_triple_axle_tonperhv,
            bridge_formula_cnt,
            bridge_formula_olhv_perc,
            bridge_formula_tonperhv,
            gross_formula_cnt,
            gross_formula_olhv_perc,
            gross_formula_tonperhv,
            total_avg_cnt,
            total_avg_olhv_perc,
            total_avg_tonperhv,
            worst_steering_single_axle_cnt_positive_direciton,
            worst_steering_single_axle_olhv_perc_positive_direciton,
            worst_steering_single_axle_tonperhv_positive_direciton,
            worst_steering_double_axle_cnt_positive_direciton,
            worst_steering_double_axle_olhv_perc_positive_direciton,
            worst_steering_double_axle_tonperhv_positive_direciton,
            worst_non_steering_single_axle_cnt_positive_direciton,
            worst_non_steering_single_axle_olhv_perc_positive_direciton,
            worst_non_steering_single_axle_tonperhv_positive_direciton,
            worst_non_steering_double_axle_cnt_positive_direciton,
            worst_non_steering_double_axle_olhv_perc_positive_direciton,
            worst_non_steering_double_axle_tonperhv_positive_direciton,
            worst_triple_axle_cnt_positive_direciton,
            worst_triple_axle_olhv_perc_positive_direciton,
            worst_triple_axle_tonperhv_positive_direciton,
            bridge_formula_cnt_positive_direciton,
            bridge_formula_olhv_perc_positive_direciton,
            bridge_formula_tonperhv_positive_direciton,
            gross_formula_cnt_positive_direciton,
            gross_formula_olhv_perc_positive_direciton,
            gross_formula_tonperhv_positive_direciton,
            total_avg_cnt_positive_direciton,
            total_avg_olhv_perc_positive_direciton,
            total_avg_tonperhv_positive_direciton,
            worst_steering_single_axle_cnt_negative_direciton,
            worst_steering_single_axle_olhv_perc_negative_direciton,
            worst_steering_single_axle_tonperhv_negative_direciton,
            worst_steering_double_axle_cnt_negative_direciton,
            worst_steering_double_axle_olhv_perc_negative_direciton,
            worst_steering_double_axle_tonperhv_negative_direciton,
            worst_non_steering_single_axle_cnt_negative_direciton,
            worst_non_steering_single_axle_olhv_perc_negative_direciton,
            worst_non_steering_single_axle_tonperhv_negative_direciton,
            worst_non_steering_double_axle_cnt_negative_direciton,
            worst_non_steering_double_axle_olhv_perc_negative_direciton,
            worst_non_steering_double_axle_tonperhv_negative_direciton,
            worst_triple_axle_cnt_negative_direciton,
            worst_triple_axle_olhv_perc_negative_direciton,
            worst_triple_axle_tonperhv_negative_direciton,
            bridge_formula_cnt_negative_direciton,
            bridge_formula_olhv_perc_negative_direciton,
            bridge_formula_tonperhv_negative_direciton,
            gross_formula_cnt_negative_direciton,
            gross_formula_olhv_perc_negative_direciton,
            gross_formula_tonperhv_negative_direciton,
            total_avg_cnt_negative_direciton,
            total_avg_olhv_perc_negative_direciton,
            total_avg_tonperhv_negative_direciton,
            egrl_percent_positive_direction,
            egrl_percent_negative_direction,
            egrw_percent_positive_direction,
            egrw_percent_negative_direction,
            num_weighed,
            num_weighed_positive_direction,
            num_weighed_negative_direction,
            total_vehicles_cls_4tot,
            weighed_vehicles_cls_4tot,
            perc_truck_dist_cls_4tot,
            total_axles_cls_4tot,
            axles_over9t_cls_4tot,
            total_mass_cls_4tot,
            cnt_mass_over9t_cls_4tot,
            eal_pervehicle_cls_4tot,
            total_eal_cls_4tot,
            total_eal_over9t_cls_4tot,
            total_vehicles_cls_5tot,
            weighed_vehicles_cls_5tot,
            perc_truck_dist_cls_5tot,
            total_axles_cls_5tot,
            axles_over9t_cls_5tot,
            total_mass_cls_5tot,
            cnt_mass_over9t_cls_5tot,
            eal_pervehicle_cls_5tot,
            total_eal_cls_5tot,
            total_eal_over9t_cls_5tot,
            total_vehicles_cls_6tot,
            weighed_vehicles_cls_6tot,
            perc_truck_dist_cls_6tot,
            total_axles_cls_6tot,
            axles_over9t_cls_6tot,
            total_mass_cls_6tot,
            cnt_mass_over9t_cls_6tot,
            eal_pervehicle_cls_6tot,
            total_eal_cls_6tot,
            total_eal_over9t_cls_6tot,
            total_vehicles_cls_7tot,
            weighed_vehicles_cls_7tot,
            perc_truck_dist_cls_7tot,
            total_axles_cls_7tot,
            axles_over9t_cls_7tot,
            total_mass_cls_7tot,
            cnt_mass_over9t_cls_7tot,
            eal_pervehicle_cls_7tot,
            total_eal_cls_7tot,
            total_eal_over9t_cls_7tot,
            total_vehicles_cls_8tot,
            weighed_vehicles_cls_8tot,
            perc_truck_dist_cls_8tot,
            total_axles_cls_8tot,
            axles_over9t_cls_8tot,
            total_mass_cls_8tot,
            cnt_mass_over9t_cls_8tot,
            eal_pervehicle_cls_8tot,
            total_eal_cls_8tot,
            total_eal_over9t_cls_8tot,
            total_vehicles_cls_9tot,
            weighed_vehicles_cls_9tot,
            perc_truck_dist_cls_9tot,
            total_axles_cls_9tot,
            axles_over9t_cls_9tot,
            total_mass_cls_9tot,
            cnt_mass_over9t_cls_9tot,
            eal_pervehicle_cls_9tot,
            total_eal_cls_9tot,
            total_eal_over9t_cls_9tot,
            total_vehicles_cls_10tot,
            weighed_vehicles_cls_10tot,
            perc_truck_dist_cls_10tot,
            total_axles_cls_10tot,
            axles_over9t_cls_10tot,
            total_mass_cls_10tot,
            cnt_mass_over9t_cls_10tot,
            eal_pervehicle_cls_10tot,
            total_eal_cls_10tot,
            total_eal_over9t_cls_10tot,
            total_vehicles_cls_11tot,
            weighed_vehicles_cls_11tot,
            perc_truck_dist_cls_11tot,
            total_axles_cls_11tot,
            axles_over9t_cls_11tot,
            total_mass_cls_11tot,
            cnt_mass_over9t_cls_11tot,
            eal_pervehicle_cls_11tot,
            total_eal_cls_11tot,
            total_eal_over9t_cls_11tot,
            total_vehicles_cls_12tot,
            weighed_vehicles_cls_12tot,
            perc_truck_dist_cls_12tot,
            total_axles_cls_12tot,
            axles_over9t_cls_12tot,
            total_mass_cls_12tot,
            cnt_mass_over9t_cls_12tot,
            eal_pervehicle_cls_12tot,
            total_eal_cls_12tot,
            total_eal_over9t_cls_12tot,
            total_vehicles_cls_13tot,
            weighed_vehicles_cls_13tot,
            perc_truck_dist_cls_13tot,
            total_axles_cls_13tot,
            axles_over9t_cls_13tot,
            total_mass_cls_13tot,
            cnt_mass_over9t_cls_13tot,
            eal_pervehicle_cls_13tot,
            total_eal_cls_13tot,
            total_eal_over9t_cls_13tot,
            total_vehicles_cls_14tot,
            weighed_vehicles_cls_14tot,
            perc_truck_dist_cls_14tot,
            total_axles_cls_14tot,
            axles_over9t_cls_14tot,
            total_mass_cls_14tot,
            cnt_mass_over9t_cls_14tot,
            eal_pervehicle_cls_14tot,
            total_eal_cls_14tot,
            total_eal_over9t_cls_14tot,
            total_vehicles_cls_15tot,
            weighed_vehicles_cls_15tot,
            perc_truck_dist_cls_15tot,
            total_axles_cls_15tot,
            axles_over9t_cls_15tot,
            total_mass_cls_15tot,
            cnt_mass_over9t_cls_15tot,
            eal_pervehicle_cls_15tot,
            total_eal_cls_15tot,
            total_eal_over9t_cls_15tot,
            total_vehicles_cls_16tot,
            weighed_vehicles_cls_16tot,
            perc_truck_dist_cls_16tot,
            total_axles_cls_16tot,
            axles_over9t_cls_16tot,
            total_mass_cls_16tot,
            cnt_mass_over9t_cls_16tot,
            eal_pervehicle_cls_16tot,
            total_eal_cls_16tot,
            total_eal_over9t_cls_16tot,
            total_vehicles_cls_17tot,
            weighed_vehicles_cls_17tot,
            perc_truck_dist_cls_17tot,
            total_axles_cls_17tot,
            axles_over9t_cls_17tot,
            total_mass_cls_17tot,
            cnt_mass_over9t_cls_17tot,
            eal_pervehicle_cls_17tot,
            total_eal_cls_17tot,
            total_eal_over9t_cls_17tot,
            total_vehicles_cls_4p,
            weighed_vehicles_cls_4p,
            perc_truck_dist_cls_4p,
            total_axles_cls_4p,
            axles_over9t_cls_4p,
            total_mass_cls_4p,
            cnt_mass_over9t_cls_4p,
            eal_pervehicle_cls_4p,
            total_eal_cls_4p,
            total_eal_over9t_cls_4p,
            total_vehicles_cls_5p,
            weighed_vehicles_cls_5p,
            perc_truck_dist_cls_5p,
            total_axles_cls_5p,
            axles_over9t_cls_5p,
            total_mass_cls_5p,
            cnt_mass_over9t_cls_5p,
            eal_pervehicle_cls_5p,
            total_eal_cls_5p,
            total_eal_over9t_cls_5p,
            total_vehicles_cls_6p,
            weighed_vehicles_cls_6p,
            perc_truck_dist_cls_6p,
            total_axles_cls_6p,
            axles_over9t_cls_6p,
            total_mass_cls_6p,
            cnt_mass_over9t_cls_6p,
            eal_pervehicle_cls_6p,
            total_eal_cls_6p,
            total_eal_over9t_cls_6p,
            total_vehicles_cls_7p,
            weighed_vehicles_cls_7p,
            perc_truck_dist_cls_7p,
            total_axles_cls_7p,
            axles_over9t_cls_7p,
            total_mass_cls_7p,
            cnt_mass_over9t_cls_7p,
            eal_pervehicle_cls_7p,
            total_eal_cls_7p,
            total_eal_over9t_cls_7p,
            total_vehicles_cls_8p,
            weighed_vehicles_cls_8p,
            perc_truck_dist_cls_8p,
            total_axles_cls_8p,
            axles_over9t_cls_8p,
            total_mass_cls_8p,
            cnt_mass_over9t_cls_8p,
            eal_pervehicle_cls_8p,
            total_eal_cls_8p,
            total_eal_over9t_cls_8p,
            total_vehicles_cls_9p,
            weighed_vehicles_cls_9p,
            perc_truck_dist_cls_9p,
            total_axles_cls_9p,
            axles_over9t_cls_9p,
            total_mass_cls_9p,
            cnt_mass_over9t_cls_9p,
            eal_pervehicle_cls_9p,
            total_eal_cls_9p,
            total_eal_over9t_cls_9p,
            total_vehicles_cls_10p,
            weighed_vehicles_cls_10p,
            perc_truck_dist_cls_10p,
            total_axles_cls_10p,
            axles_over9t_cls_10p,
            total_mass_cls_10p,
            cnt_mass_over9t_cls_10p,
            eal_pervehicle_cls_10p,
            total_eal_cls_10p,
            total_eal_over9t_cls_10p,
            total_vehicles_cls_11p,
            weighed_vehicles_cls_11p,
            perc_truck_dist_cls_11p,
            total_axles_cls_11p,
            axles_over9t_cls_11p,
            total_mass_cls_11p,
            cnt_mass_over9t_cls_11p,
            eal_pervehicle_cls_11p,
            total_eal_cls_11p,
            total_eal_over9t_cls_11p,
            total_vehicles_cls_12p,
            weighed_vehicles_cls_12p,
            perc_truck_dist_cls_12p,
            total_axles_cls_12p,
            axles_over9t_cls_12p,
            total_mass_cls_12p,
            cnt_mass_over9t_cls_12p,
            eal_pervehicle_cls_12p,
            total_eal_cls_12p,
            total_eal_over9t_cls_12p,
            total_vehicles_cls_13p,
            weighed_vehicles_cls_13p,
            perc_truck_dist_cls_13p,
            total_axles_cls_13p,
            axles_over9t_cls_13p,
            total_mass_cls_13p,
            cnt_mass_over9t_cls_13p,
            eal_pervehicle_cls_13p,
            total_eal_cls_13p,
            total_eal_over9t_cls_13p,
            total_vehicles_cls_14p,
            weighed_vehicles_cls_14p,
            perc_truck_dist_cls_14p,
            total_axles_cls_14p,
            axles_over9t_cls_14p,
            total_mass_cls_14p,
            cnt_mass_over9t_cls_14p,
            eal_pervehicle_cls_14p,
            total_eal_cls_14p,
            total_eal_over9t_cls_14p,
            total_vehicles_cls_15p,
            weighed_vehicles_cls_15p,
            perc_truck_dist_cls_15p,
            total_axles_cls_15p,
            axles_over9t_cls_15p,
            total_mass_cls_15p,
            cnt_mass_over9t_cls_15p,
            eal_pervehicle_cls_15p,
            total_eal_cls_15p,
            total_eal_over9t_cls_15p,
            total_vehicles_cls_16p,
            weighed_vehicles_cls_16p,
            perc_truck_dist_cls_16p,
            total_axles_cls_16p,
            axles_over9t_cls_16p,
            total_mass_cls_16p,
            cnt_mass_over9t_cls_16p,
            eal_pervehicle_cls_16p,
            total_eal_cls_16p,
            total_eal_over9t_cls_16p,
            total_vehicles_cls_17p,
            weighed_vehicles_cls_17p,
            perc_truck_dist_cls_17p,
            total_axles_cls_17p,
            axles_over9t_cls_17p,
            total_mass_cls_17p,
            cnt_mass_over9t_cls_17p,
            eal_pervehicle_cls_17p,
            total_eal_cls_17p,
            total_eal_over9t_cls_17p,
            total_vehicles_cls_4n,
            weighed_vehicles_cls_4n,
            perc_truck_dist_cls_4n,
            total_axles_cls_4n,
            axles_over9t_cls_4n,
            total_mass_cls_4n,
            cnt_mass_over9t_cls_4n,
            eal_pervehicle_cls_4n,
            total_eal_cls_4n,
            total_eal_over9t_cls_4n,
            total_vehicles_cls_5n,
            weighed_vehicles_cls_5n,
            perc_truck_dist_cls_5n,
            total_axles_cls_5n,
            axles_over9t_cls_5n,
            total_mass_cls_5n,
            cnt_mass_over9t_cls_5n,
            eal_pervehicle_cls_5n,
            total_eal_cls_5n,
            total_eal_over9t_cls_5n,
            total_vehicles_cls_6n,
            weighed_vehicles_cls_6n,
            perc_truck_dist_cls_6n,
            total_axles_cls_6n,
            axles_over9t_cls_6n,
            total_mass_cls_6n,
            cnt_mass_over9t_cls_6n,
            eal_pervehicle_cls_6n,
            total_eal_cls_6n,
            total_eal_over9t_cls_6n,
            total_vehicles_cls_7n,
            weighed_vehicles_cls_7n,
            perc_truck_dist_cls_7n,
            total_axles_cls_7n,
            axles_over9t_cls_7n,
            total_mass_cls_7n,
            cnt_mass_over9t_cls_7n,
            eal_pervehicle_cls_7n,
            total_eal_cls_7n,
            total_eal_over9t_cls_7n,
            total_vehicles_cls_8n,
            weighed_vehicles_cls_8n,
            perc_truck_dist_cls_8n,
            total_axles_cls_8n,
            axles_over9t_cls_8n,
            total_mass_cls_8n,
            cnt_mass_over9t_cls_8n,
            eal_pervehicle_cls_8n,
            total_eal_cls_8n,
            total_eal_over9t_cls_8n,
            total_vehicles_cls_9n,
            weighed_vehicles_cls_9n,
            perc_truck_dist_cls_9n,
            total_axles_cls_9n,
            axles_over9t_cls_9n,
            total_mass_cls_9n,
            cnt_mass_over9t_cls_9n,
            eal_pervehicle_cls_9n,
            total_eal_cls_9n,
            total_eal_over9t_cls_9n,
            total_vehicles_cls_10n,
            weighed_vehicles_cls_10n,
            perc_truck_dist_cls_10n,
            total_axles_cls_10n,
            axles_over9t_cls_10n,
            total_mass_cls_10n,
            cnt_mass_over9t_cls_10n,
            eal_pervehicle_cls_10n,
            total_eal_cls_10n,
            total_eal_over9t_cls_10n,
            total_vehicles_cls_11n,
            weighed_vehicles_cls_11n,
            perc_truck_dist_cls_11n,
            total_axles_cls_11n,
            axles_over9t_cls_11n,
            total_mass_cls_11n,
            cnt_mass_over9t_cls_11n,
            eal_pervehicle_cls_11n,
            total_eal_cls_11n,
            total_eal_over9t_cls_11n,
            total_vehicles_cls_12n,
            weighed_vehicles_cls_12n,
            perc_truck_dist_cls_12n,
            total_axles_cls_12n,
            axles_over9t_cls_12n,
            total_mass_cls_12n,
            cnt_mass_over9t_cls_12n,
            eal_pervehicle_cls_12n,
            total_eal_cls_12n,
            total_eal_over9t_cls_12n,
            total_vehicles_cls_13n,
            weighed_vehicles_cls_13n,
            perc_truck_dist_cls_13n,
            total_axles_cls_13n,
            axles_over9t_cls_13n,
            total_mass_cls_13n,
            cnt_mass_over9t_cls_13n,
            eal_pervehicle_cls_13n,
            total_eal_cls_13n,
            total_eal_over9t_cls_13n,
            total_vehicles_cls_14n,
            weighed_vehicles_cls_14n,
            perc_truck_dist_cls_14n,
            total_axles_cls_14n,
            axles_over9t_cls_14n,
            total_mass_cls_14n,
            cnt_mass_over9t_cls_14n,
            eal_pervehicle_cls_14n,
            total_eal_cls_14n,
            total_eal_over9t_cls_14n,
            total_vehicles_cls_15n,
            weighed_vehicles_cls_15n,
            perc_truck_dist_cls_15n,
            total_axles_cls_15n,
            axles_over9t_cls_15n,
            total_mass_cls_15n,
            cnt_mass_over9t_cls_15n,
            eal_pervehicle_cls_15n,
            total_eal_cls_15n,
            total_eal_over9t_cls_15n,
            total_vehicles_cls_16n,
            weighed_vehicles_cls_16n,
            perc_truck_dist_cls_16n,
            total_axles_cls_16n,
            axles_over9t_cls_16n,
            total_mass_cls_16n,
            cnt_mass_over9t_cls_16n,
            eal_pervehicle_cls_16n,
            total_eal_cls_16n,
            total_eal_over9t_cls_16n,
            total_vehicles_cls_17n,
            weighed_vehicles_cls_17n,
            perc_truck_dist_cls_17n,
            total_axles_cls_17n,
            axles_over9t_cls_17n,
            total_mass_cls_17n,
            cnt_mass_over9t_cls_17n,
            eal_pervehicle_cls_17n,
            total_eal_cls_17n,
            total_eal_over9t_cls_17n
            )
        VALUES(
            '{header_id}',
            {self.egrl_percent},
            {self.egrw_percent},
            {self.mean_equivalent_axle_mass},
            {self.mean_equivalent_axle_mass_positive_direction},
            {self.mean_equivalent_axle_mass_negative_direction},
            {self.mean_axle_spacing},
            {self.mean_axle_spacing_positive_direction},
            {self.mean_axle_spacing_negative_direction},
            {self.e80_per_axle},
            {self.e80_per_axle_positive_direction},
            {self.e80_per_axle_negative_direction},
            {self.olhv},
            {self.olhv_positive_direction},
            {self.olhv_negative_direction},
            {self.olhv_percent},
            {self.olhv_percent_positive_direction},
            {self.olhv_percent_negative_direction},
            {self.tonnage_generated},
            {self.tonnage_generated_positive_direction},
            {self.tonnage_generated_negative_direction},
            {self.olton},
            {self.olton_positive_direction},
            {self.olton_negative_direction},
            {self.olton_percent},
            {self.olton_percent_positive_direction},
            {self.olton_percent_negative_direction},
            {self.ole80},
            {self.ole80_positive_direction},
            {self.ole80_negative_direction},
            {self.ole80_percent},
            {self.ole80_percent_positive_direction},
            {self.ole80_percent_negative_direction},
            {self.xe80},
            {self.xe80_positive_direction},
            {self.xe80_negative_direction},
            {self.xe80_percent},
            {self.xe80_percent_positive_direction},
            {self.xe80_percent_negative_direction},
            {self.e80_per_day},
            {self.e80_per_day_positive_direction},
            {self.e80_per_day_negative_direction},
            {self.e80_per_heavy_vehicle},
            {self.e80_per_heavy_vehicle_positive_direction},
            {self.e80_per_heavy_vehicle_negative_direction},
            {self.worst_steering_single_axle_cnt},
            {self.worst_steering_single_axle_olhv_perc},
            {self.worst_steering_single_axle_tonperhv},
            {self.worst_steering_double_axle_cnt},
            {self.worst_steering_double_axle_olhv_perc},
            {self.worst_steering_double_axle_tonperhv},
            {self.worst_non_steering_single_axle_cnt},
            {self.worst_non_steering_single_axle_olhv_perc},
            {self.worst_non_steering_single_axle_tonperhv},
            {self.worst_non_steering_double_axle_cnt},
            {self.worst_non_steering_double_axle_olhv_perc},
            {self.worst_non_steering_double_axle_tonperhv},
            {self.worst_triple_axle_cnt},
            {self.worst_triple_axle_olhv_perc},
            {self.worst_triple_axle_tonperhv},
            {self.bridge_formula_cnt},
            {self.bridge_formula_olhv_perc},
            {self.bridge_formula_tonperhv},
            {self.gross_formula_cnt},
            {self.gross_formula_olhv_perc},
            {self.gross_formula_tonperhv},
            {self.total_avg_cnt},
            {self.total_avg_olhv_perc},
            {self.total_avg_tonperhv},
            {self.worst_steering_single_axle_cnt_positive_direciton},
            {self.worst_steering_single_axle_olhv_perc_positive_direciton},
            {self.worst_steering_single_axle_tonperhv_positive_direciton},
            {self.worst_steering_double_axle_cnt_positive_direciton},
            {self.worst_steering_double_axle_olhv_perc_positive_direciton},
            {self.worst_steering_double_axle_tonperhv_positive_direciton},
            {self.worst_non_steering_single_axle_cnt_positive_direciton},
            {self.worst_non_steering_single_axle_olhv_perc_positive_direciton},
            {self.worst_non_steering_single_axle_tonperhv_positive_direciton},
            {self.worst_non_steering_double_axle_cnt_positive_direciton},
            {self.worst_non_steering_double_axle_olhv_perc_positive_direciton},
            {self.worst_non_steering_double_axle_tonperhv_positive_direciton},
            {self.worst_triple_axle_cnt_positive_direciton},
            {self.worst_triple_axle_olhv_perc_positive_direciton},
            {self.worst_triple_axle_tonperhv_positive_direciton},
            {self.bridge_formula_cnt_positive_direciton},
            {self.bridge_formula_olhv_perc_positive_direciton},
            {self.bridge_formula_tonperhv_positive_direciton},
            {self.gross_formula_cnt_positive_direciton},
            {self.gross_formula_olhv_perc_positive_direciton},
            {self.gross_formula_tonperhv_positive_direciton},
            {self.total_avg_cnt_positive_direciton},
            {self.total_avg_olhv_perc_positive_direciton},
            {self.total_avg_tonperhv_positive_direciton},
            {self.worst_steering_single_axle_cnt_negative_direciton},
            {self.worst_steering_single_axle_olhv_perc_negative_direciton},
            {self.worst_steering_single_axle_tonperhv_negative_direciton},
            {self.worst_steering_double_axle_cnt_negative_direciton},
            {self.worst_steering_double_axle_olhv_perc_negative_direciton},
            {self.worst_steering_double_axle_tonperhv_negative_direciton},
            {self.worst_non_steering_single_axle_cnt_negative_direciton},
            {self.worst_non_steering_single_axle_olhv_perc_negative_direciton},
            {self.worst_non_steering_single_axle_tonperhv_negative_direciton},
            {self.worst_non_steering_double_axle_cnt_negative_direciton},
            {self.worst_non_steering_double_axle_olhv_perc_negative_direciton},
            {self.worst_non_steering_double_axle_tonperhv_negative_direciton},
            {self.worst_triple_axle_cnt_negative_direciton},
            {self.worst_triple_axle_olhv_perc_negative_direciton},
            {self.worst_triple_axle_tonperhv_negative_direciton},
            {self.bridge_formula_cnt_negative_direciton},
            {self.bridge_formula_olhv_perc_negative_direciton},
            {self.bridge_formula_tonperhv_negative_direciton},
            {self.gross_formula_cnt_negative_direciton},
            {self.gross_formula_olhv_perc_negative_direciton},
            {self.gross_formula_tonperhv_negative_direciton},
            {self.total_avg_cnt_negative_direciton},
            {self.total_avg_olhv_perc_negative_direciton},
            {self.total_avg_tonperhv_negative_direciton},
            {self.egrl_percent_positive_direction},
            {self.egrl_percent_negative_direction},
            {self.egrw_percent_positive_direction},
            {self.egrw_percent_negative_direction},
            {self.num_weighed},
            {self.num_weighed_positive_direction},
            {self.num_weighed_negative_direction},
            {self.total_vehicles_cls_4tot},
            {self.weighed_vehicles_cls_4tot},
            {self.perc_truck_dist_cls_4tot},
            {self.total_axles_cls_4tot},
            {self.axles_over9t_cls_4tot},
            {self.total_mass_cls_4tot},
            {self.cnt_mass_over9t_cls_4tot},
            {self.eal_pervehicle_cls_4tot},
            {self.total_eal_cls_4tot},
            {self.total_eal_over9t_cls_4tot},
            {self.total_vehicles_cls_5tot},
            {self.weighed_vehicles_cls_5tot},
            {self.perc_truck_dist_cls_5tot},
            {self.total_axles_cls_5tot},
            {self.axles_over9t_cls_5tot},
            {self.total_mass_cls_5tot},
            {self.cnt_mass_over9t_cls_5tot},
            {self.eal_pervehicle_cls_5tot},
            {self.total_eal_cls_5tot},
            {self.total_eal_over9t_cls_5tot},
            {self.total_vehicles_cls_6tot},
            {self.weighed_vehicles_cls_6tot},
            {self.perc_truck_dist_cls_6tot},
            {self.total_axles_cls_6tot},
            {self.axles_over9t_cls_6tot},
            {self.total_mass_cls_6tot},
            {self.cnt_mass_over9t_cls_6tot},
            {self.eal_pervehicle_cls_6tot},
            {self.total_eal_cls_6tot},
            {self.total_eal_over9t_cls_6tot},
            {self.total_vehicles_cls_7tot},
            {self.weighed_vehicles_cls_7tot},
            {self.perc_truck_dist_cls_7tot},
            {self.total_axles_cls_7tot},
            {self.axles_over9t_cls_7tot},
            {self.total_mass_cls_7tot},
            {self.cnt_mass_over9t_cls_7tot},
            {self.eal_pervehicle_cls_7tot},
            {self.total_eal_cls_7tot},
            {self.total_eal_over9t_cls_7tot},
            {self.total_vehicles_cls_8tot},
            {self.weighed_vehicles_cls_8tot},
            {self.perc_truck_dist_cls_8tot},
            {self.total_axles_cls_8tot},
            {self.axles_over9t_cls_8tot},
            {self.total_mass_cls_8tot},
            {self.cnt_mass_over9t_cls_8tot},
            {self.eal_pervehicle_cls_8tot},
            {self.total_eal_cls_8tot},
            {self.total_eal_over9t_cls_8tot},
            {self.total_vehicles_cls_9tot},
            {self.weighed_vehicles_cls_9tot},
            {self.perc_truck_dist_cls_9tot},
            {self.total_axles_cls_9tot},
            {self.axles_over9t_cls_9tot},
            {self.total_mass_cls_9tot},
            {self.cnt_mass_over9t_cls_9tot},
            {self.eal_pervehicle_cls_9tot},
            {self.total_eal_cls_9tot},
            {self.total_eal_over9t_cls_9tot},
            {self.total_vehicles_cls_10tot},
            {self.weighed_vehicles_cls_10tot},
            {self.perc_truck_dist_cls_10tot},
            {self.total_axles_cls_10tot},
            {self.axles_over9t_cls_10tot},
            {self.total_mass_cls_10tot},
            {self.cnt_mass_over9t_cls_10tot},
            {self.eal_pervehicle_cls_10tot},
            {self.total_eal_cls_10tot},
            {self.total_eal_over9t_cls_10tot},
            {self.total_vehicles_cls_11tot},
            {self.weighed_vehicles_cls_11tot},
            {self.perc_truck_dist_cls_11tot},
            {self.total_axles_cls_11tot},
            {self.axles_over9t_cls_11tot},
            {self.total_mass_cls_11tot},
            {self.cnt_mass_over9t_cls_11tot},
            {self.eal_pervehicle_cls_11tot},
            {self.total_eal_cls_11tot},
            {self.total_eal_over9t_cls_11tot},
            {self.total_vehicles_cls_12tot},
            {self.weighed_vehicles_cls_12tot},
            {self.perc_truck_dist_cls_12tot},
            {self.total_axles_cls_12tot},
            {self.axles_over9t_cls_12tot},
            {self.total_mass_cls_12tot},
            {self.cnt_mass_over9t_cls_12tot},
            {self.eal_pervehicle_cls_12tot},
            {self.total_eal_cls_12tot},
            {self.total_eal_over9t_cls_12tot},
            {self.total_vehicles_cls_13tot},
            {self.weighed_vehicles_cls_13tot},
            {self.perc_truck_dist_cls_13tot},
            {self.total_axles_cls_13tot},
            {self.axles_over9t_cls_13tot},
            {self.total_mass_cls_13tot},
            {self.cnt_mass_over9t_cls_13tot},
            {self.eal_pervehicle_cls_13tot},
            {self.total_eal_cls_13tot},
            {self.total_eal_over9t_cls_13tot},
            {self.total_vehicles_cls_14tot},
            {self.weighed_vehicles_cls_14tot},
            {self.perc_truck_dist_cls_14tot},
            {self.total_axles_cls_14tot},
            {self.axles_over9t_cls_14tot},
            {self.total_mass_cls_14tot},
            {self.cnt_mass_over9t_cls_14tot},
            {self.eal_pervehicle_cls_14tot},
            {self.total_eal_cls_14tot},
            {self.total_eal_over9t_cls_14tot},
            {self.total_vehicles_cls_15tot},
            {self.weighed_vehicles_cls_15tot},
            {self.perc_truck_dist_cls_15tot},
            {self.total_axles_cls_15tot},
            {self.axles_over9t_cls_15tot},
            {self.total_mass_cls_15tot},
            {self.cnt_mass_over9t_cls_15tot},
            {self.eal_pervehicle_cls_15tot},
            {self.total_eal_cls_15tot},
            {self.total_eal_over9t_cls_15tot},
            {self.total_vehicles_cls_16tot},
            {self.weighed_vehicles_cls_16tot},
            {self.perc_truck_dist_cls_16tot},
            {self.total_axles_cls_16tot},
            {self.axles_over9t_cls_16tot},
            {self.total_mass_cls_16tot},
            {self.cnt_mass_over9t_cls_16tot},
            {self.eal_pervehicle_cls_16tot},
            {self.total_eal_cls_16tot},
            {self.total_eal_over9t_cls_16tot},
            {self.total_vehicles_cls_17tot},
            {self.weighed_vehicles_cls_17tot},
            {self.perc_truck_dist_cls_17tot},
            {self.total_axles_cls_17tot},
            {self.axles_over9t_cls_17tot},
            {self.total_mass_cls_17tot},
            {self.cnt_mass_over9t_cls_17tot},
            {self.eal_pervehicle_cls_17tot},
            {self.total_eal_cls_17tot},
            {self.total_eal_over9t_cls_17tot},
            {self.total_vehicles_cls_4p},
            {self.weighed_vehicles_cls_4p},
            {self.perc_truck_dist_cls_4p},
            {self.total_axles_cls_4p},
            {self.axles_over9t_cls_4p},
            {self.total_mass_cls_4p},
            {self.cnt_mass_over9t_cls_4p},
            {self.eal_pervehicle_cls_4p},
            {self.total_eal_cls_4p},
            {self.total_eal_over9t_cls_4p},
            {self.total_vehicles_cls_5p},
            {self.weighed_vehicles_cls_5p},
            {self.perc_truck_dist_cls_5p},
            {self.total_axles_cls_5p},
            {self.axles_over9t_cls_5p},
            {self.total_mass_cls_5p},
            {self.cnt_mass_over9t_cls_5p},
            {self.eal_pervehicle_cls_5p},
            {self.total_eal_cls_5p},
            {self.total_eal_over9t_cls_5p},
            {self.total_vehicles_cls_6p},
            {self.weighed_vehicles_cls_6p},
            {self.perc_truck_dist_cls_6p},
            {self.total_axles_cls_6p},
            {self.axles_over9t_cls_6p},
            {self.total_mass_cls_6p},
            {self.cnt_mass_over9t_cls_6p},
            {self.eal_pervehicle_cls_6p},
            {self.total_eal_cls_6p},
            {self.total_eal_over9t_cls_6p},
            {self.total_vehicles_cls_7p},
            {self.weighed_vehicles_cls_7p},
            {self.perc_truck_dist_cls_7p},
            {self.total_axles_cls_7p},
            {self.axles_over9t_cls_7p},
            {self.total_mass_cls_7p},
            {self.cnt_mass_over9t_cls_7p},
            {self.eal_pervehicle_cls_7p},
            {self.total_eal_cls_7p},
            {self.total_eal_over9t_cls_7p},
            {self.total_vehicles_cls_8p},
            {self.weighed_vehicles_cls_8p},
            {self.perc_truck_dist_cls_8p},
            {self.total_axles_cls_8p},
            {self.axles_over9t_cls_8p},
            {self.total_mass_cls_8p},
            {self.cnt_mass_over9t_cls_8p},
            {self.eal_pervehicle_cls_8p},
            {self.total_eal_cls_8p},
            {self.total_eal_over9t_cls_8p},
            {self.total_vehicles_cls_9p},
            {self.weighed_vehicles_cls_9p},
            {self.perc_truck_dist_cls_9p},
            {self.total_axles_cls_9p},
            {self.axles_over9t_cls_9p},
            {self.total_mass_cls_9p},
            {self.cnt_mass_over9t_cls_9p},
            {self.eal_pervehicle_cls_9p},
            {self.total_eal_cls_9p},
            {self.total_eal_over9t_cls_9p},
            {self.total_vehicles_cls_10p},
            {self.weighed_vehicles_cls_10p},
            {self.perc_truck_dist_cls_10p},
            {self.total_axles_cls_10p},
            {self.axles_over9t_cls_10p},
            {self.total_mass_cls_10p},
            {self.cnt_mass_over9t_cls_10p},
            {self.eal_pervehicle_cls_10p},
            {self.total_eal_cls_10p},
            {self.total_eal_over9t_cls_10p},
            {self.total_vehicles_cls_11p},
            {self.weighed_vehicles_cls_11p},
            {self.perc_truck_dist_cls_11p},
            {self.total_axles_cls_11p},
            {self.axles_over9t_cls_11p},
            {self.total_mass_cls_11p},
            {self.cnt_mass_over9t_cls_11p},
            {self.eal_pervehicle_cls_11p},
            {self.total_eal_cls_11p},
            {self.total_eal_over9t_cls_11p},
            {self.total_vehicles_cls_12p},
            {self.weighed_vehicles_cls_12p},
            {self.perc_truck_dist_cls_12p},
            {self.total_axles_cls_12p},
            {self.axles_over9t_cls_12p},
            {self.total_mass_cls_12p},
            {self.cnt_mass_over9t_cls_12p},
            {self.eal_pervehicle_cls_12p},
            {self.total_eal_cls_12p},
            {self.total_eal_over9t_cls_12p},
            {self.total_vehicles_cls_13p},
            {self.weighed_vehicles_cls_13p},
            {self.perc_truck_dist_cls_13p},
            {self.total_axles_cls_13p},
            {self.axles_over9t_cls_13p},
            {self.total_mass_cls_13p},
            {self.cnt_mass_over9t_cls_13p},
            {self.eal_pervehicle_cls_13p},
            {self.total_eal_cls_13p},
            {self.total_eal_over9t_cls_13p},
            {self.total_vehicles_cls_14p},
            {self.weighed_vehicles_cls_14p},
            {self.perc_truck_dist_cls_14p},
            {self.total_axles_cls_14p},
            {self.axles_over9t_cls_14p},
            {self.total_mass_cls_14p},
            {self.cnt_mass_over9t_cls_14p},
            {self.eal_pervehicle_cls_14p},
            {self.total_eal_cls_14p},
            {self.total_eal_over9t_cls_14p},
            {self.total_vehicles_cls_15p},
            {self.weighed_vehicles_cls_15p},
            {self.perc_truck_dist_cls_15p},
            {self.total_axles_cls_15p},
            {self.axles_over9t_cls_15p},
            {self.total_mass_cls_15p},
            {self.cnt_mass_over9t_cls_15p},
            {self.eal_pervehicle_cls_15p},
            {self.total_eal_cls_15p},
            {self.total_eal_over9t_cls_15p},
            {self.total_vehicles_cls_16p},
            {self.weighed_vehicles_cls_16p},
            {self.perc_truck_dist_cls_16p},
            {self.total_axles_cls_16p},
            {self.axles_over9t_cls_16p},
            {self.total_mass_cls_16p},
            {self.cnt_mass_over9t_cls_16p},
            {self.eal_pervehicle_cls_16p},
            {self.total_eal_cls_16p},
            {self.total_eal_over9t_cls_16p},
            {self.total_vehicles_cls_17p},
            {self.weighed_vehicles_cls_17p},
            {self.perc_truck_dist_cls_17p},
            {self.total_axles_cls_17p},
            {self.axles_over9t_cls_17p},
            {self.total_mass_cls_17p},
            {self.cnt_mass_over9t_cls_17p},
            {self.eal_pervehicle_cls_17p},
            {self.total_eal_cls_17p},
            {self.total_eal_over9t_cls_17p},
            {self.total_vehicles_cls_4n},
            {self.weighed_vehicles_cls_4n},
            {self.perc_truck_dist_cls_4n},
            {self.total_axles_cls_4n},
            {self.axles_over9t_cls_4n},
            {self.total_mass_cls_4n},
            {self.cnt_mass_over9t_cls_4n},
            {self.eal_pervehicle_cls_4n},
            {self.total_eal_cls_4n},
            {self.total_eal_over9t_cls_4n},
            {self.total_vehicles_cls_5n},
            {self.weighed_vehicles_cls_5n},
            {self.perc_truck_dist_cls_5n},
            {self.total_axles_cls_5n},
            {self.axles_over9t_cls_5n},
            {self.total_mass_cls_5n},
            {self.cnt_mass_over9t_cls_5n},
            {self.eal_pervehicle_cls_5n},
            {self.total_eal_cls_5n},
            {self.total_eal_over9t_cls_5n},
            {self.total_vehicles_cls_6n},
            {self.weighed_vehicles_cls_6n},
            {self.perc_truck_dist_cls_6n},
            {self.total_axles_cls_6n},
            {self.axles_over9t_cls_6n},
            {self.total_mass_cls_6n},
            {self.cnt_mass_over9t_cls_6n},
            {self.eal_pervehicle_cls_6n},
            {self.total_eal_cls_6n},
            {self.total_eal_over9t_cls_6n},
            {self.total_vehicles_cls_7n},
            {self.weighed_vehicles_cls_7n},
            {self.perc_truck_dist_cls_7n},
            {self.total_axles_cls_7n},
            {self.axles_over9t_cls_7n},
            {self.total_mass_cls_7n},
            {self.cnt_mass_over9t_cls_7n},
            {self.eal_pervehicle_cls_7n},
            {self.total_eal_cls_7n},
            {self.total_eal_over9t_cls_7n},
            {self.total_vehicles_cls_8n},
            {self.weighed_vehicles_cls_8n},
            {self.perc_truck_dist_cls_8n},
            {self.total_axles_cls_8n},
            {self.axles_over9t_cls_8n},
            {self.total_mass_cls_8n},
            {self.cnt_mass_over9t_cls_8n},
            {self.eal_pervehicle_cls_8n},
            {self.total_eal_cls_8n},
            {self.total_eal_over9t_cls_8n},
            {self.total_vehicles_cls_9n},
            {self.weighed_vehicles_cls_9n},
            {self.perc_truck_dist_cls_9n},
            {self.total_axles_cls_9n},
            {self.axles_over9t_cls_9n},
            {self.total_mass_cls_9n},
            {self.cnt_mass_over9t_cls_9n},
            {self.eal_pervehicle_cls_9n},
            {self.total_eal_cls_9n},
            {self.total_eal_over9t_cls_9n},
            {self.total_vehicles_cls_10n},
            {self.weighed_vehicles_cls_10n},
            {self.perc_truck_dist_cls_10n},
            {self.total_axles_cls_10n},
            {self.axles_over9t_cls_10n},
            {self.total_mass_cls_10n},
            {self.cnt_mass_over9t_cls_10n},
            {self.eal_pervehicle_cls_10n},
            {self.total_eal_cls_10n},
            {self.total_eal_over9t_cls_10n},
            {self.total_vehicles_cls_11n},
            {self.weighed_vehicles_cls_11n},
            {self.perc_truck_dist_cls_11n},
            {self.total_axles_cls_11n},
            {self.axles_over9t_cls_11n},
            {self.total_mass_cls_11n},
            {self.cnt_mass_over9t_cls_11n},
            {self.eal_pervehicle_cls_11n},
            {self.total_eal_cls_11n},
            {self.total_eal_over9t_cls_11n},
            {self.total_vehicles_cls_12n},
            {self.weighed_vehicles_cls_12n},
            {self.perc_truck_dist_cls_12n},
            {self.total_axles_cls_12n},
            {self.axles_over9t_cls_12n},
            {self.total_mass_cls_12n},
            {self.cnt_mass_over9t_cls_12n},
            {self.eal_pervehicle_cls_12n},
            {self.total_eal_cls_12n},
            {self.total_eal_over9t_cls_12n},
            {self.total_vehicles_cls_13n},
            {self.weighed_vehicles_cls_13n},
            {self.perc_truck_dist_cls_13n},
            {self.total_axles_cls_13n},
            {self.axles_over9t_cls_13n},
            {self.total_mass_cls_13n},
            {self.cnt_mass_over9t_cls_13n},
            {self.eal_pervehicle_cls_13n},
            {self.total_eal_cls_13n},
            {self.total_eal_over9t_cls_13n},
            {self.total_vehicles_cls_14n},
            {self.weighed_vehicles_cls_14n},
            {self.perc_truck_dist_cls_14n},
            {self.total_axles_cls_14n},
            {self.axles_over9t_cls_14n},
            {self.total_mass_cls_14n},
            {self.cnt_mass_over9t_cls_14n},
            {self.eal_pervehicle_cls_14n},
            {self.total_eal_cls_14n},
            {self.total_eal_over9t_cls_14n},
            {self.total_vehicles_cls_15n},
            {self.weighed_vehicles_cls_15n},
            {self.perc_truck_dist_cls_15n},
            {self.total_axles_cls_15n},
            {self.axles_over9t_cls_15n},
            {self.total_mass_cls_15n},
            {self.cnt_mass_over9t_cls_15n},
            {self.eal_pervehicle_cls_15n},
            {self.total_eal_cls_15n},
            {self.total_eal_over9t_cls_15n},
            {self.total_vehicles_cls_16n},
            {self.weighed_vehicles_cls_16n},
            {self.perc_truck_dist_cls_16n},
            {self.total_axles_cls_16n},
            {self.axles_over9t_cls_16n},
            {self.total_mass_cls_16n},
            {self.cnt_mass_over9t_cls_16n},
            {self.eal_pervehicle_cls_16n},
            {self.total_eal_cls_16n},
            {self.total_eal_over9t_cls_16n},
            {self.total_vehicles_cls_17n},
            {self.weighed_vehicles_cls_17n},
            {self.perc_truck_dist_cls_17n},
            {self.total_axles_cls_17n},
            {self.axles_over9t_cls_17n},
            {self.total_mass_cls_17n},
            {self.cnt_mass_over9t_cls_17n},
            {self.eal_pervehicle_cls_17n},
            {self.total_eal_cls_17n},
            {self.total_eal_over9t_cls_17n})
            ON CONFLICT ON CONSTRAINT electronic_count_header_hswim_pkey DO NOTHING 
            -- UPDATE SET 
            -- egrl_percent = COALESCE(EXCLUDED.egrl_percent,egrl_percent),
            -- egrw_percent = COALESCE(EXCLUDED.egrw_percent,egrw_percent),
            -- mean_equivalent_axle_mass = COALESCE(EXCLUDED.mean_equivalent_axle_mass,mean_equivalent_axle_mass),
            -- mean_equivalent_axle_mass_positive_direction = COALESCE(EXCLUDED.mean_equivalent_axle_mass_positive_direction,mean_equivalent_axle_mass_positive_direction),
            -- mean_equivalent_axle_mass_negative_direction = COALESCE(EXCLUDED.mean_equivalent_axle_mass_negative_direction,mean_equivalent_axle_mass_negative_direction),
            -- mean_axle_spacing = COALESCE(EXCLUDED.mean_axle_spacing,mean_axle_spacing),
            -- mean_axle_spacing_positive_direction = COALESCE(EXCLUDED.mean_axle_spacing_positive_direction,mean_axle_spacing_positive_direction),
            -- mean_axle_spacing_negative_direction = COALESCE(EXCLUDED.mean_axle_spacing_negative_direction,mean_axle_spacing_negative_direction),
            -- e80_per_axle = COALESCE(EXCLUDED.e80_per_axle,e80_per_axle),
            -- e80_per_axle_positive_direction = COALESCE(EXCLUDED.e80_per_axle_positive_direction,e80_per_axle_positive_direction),
            -- e80_per_axle_negative_direction = COALESCE(EXCLUDED.e80_per_axle_negative_direction,e80_per_axle_negative_direction),
            -- olhv = COALESCE(EXCLUDED.olhv,olhv),
            -- olhv_positive_direction = COALESCE(EXCLUDED.olhv_positive_direction,olhv_positive_direction),
            -- olhv_negative_direction = COALESCE(EXCLUDED.olhv_negative_direction,olhv_negative_direction),
            -- olhv_percent = COALESCE(EXCLUDED.olhv_percent,olhv_percent),
            -- olhv_percent_positive_direction = COALESCE(EXCLUDED.olhv_percent_positive_direction,olhv_percent_positive_direction),
            -- olhv_percent_negative_direction = COALESCE(EXCLUDED.olhv_percent_negative_direction,olhv_percent_negative_direction),
            -- tonnage_generated = COALESCE(EXCLUDED.tonnage_generated,tonnage_generated),
            -- tonnage_generated_positive_direction = COALESCE(EXCLUDED.tonnage_generated_positive_direction,tonnage_generated_positive_direction),
            -- tonnage_generated_negative_direction = COALESCE(EXCLUDED.tonnage_generated_negative_direction,tonnage_generated_negative_direction),
            -- olton = COALESCE(EXCLUDED.olton,olton),
            -- olton_positive_direction = COALESCE(EXCLUDED.olton_positive_direction,olton_positive_direction),
            -- olton_negative_direction = COALESCE(EXCLUDED.olton_negative_direction,olton_negative_direction),
            -- olton_percent = COALESCE(EXCLUDED.olton_percent,olton_percent),
            -- olton_percent_positive_direction = COALESCE(EXCLUDED.olton_percent_positive_direction,olton_percent_positive_direction),
            -- olton_percent_negative_direction = COALESCE(EXCLUDED.olton_percent_negative_direction,olton_percent_negative_direction),
            -- ole8 = COALESCE(EXCLUDED.ole8,ole8),
            -- ole80_positive_direction = COALESCE(EXCLUDED.ole80_positive_direction,ole80_positive_direction),
            -- ole80_negative_direction = COALESCE(EXCLUDED.ole80_negative_direction,ole80_negative_direction),
            -- ole80_percent = COALESCE(EXCLUDED.ole80_percent,ole80_percent),
            -- ole80_percent_positive_direction = COALESCE(EXCLUDED.ole80_percent_positive_direction,ole80_percent_positive_direction),
            -- ole80_percent_negative_direction = COALESCE(EXCLUDED.ole80_percent_negative_direction,ole80_percent_negative_direction),
            -- xe8 = COALESCE(EXCLUDED.xe8,xe8),
            -- xe80_positive_direction = COALESCE(EXCLUDED.xe80_positive_direction,xe80_positive_direction),
            -- xe80_negative_direction = COALESCE(EXCLUDED.xe80_negative_direction,xe80_negative_direction),
            -- xe80_percent = COALESCE(EXCLUDED.xe80_percent,xe80_percent),
            -- xe80_percent_positive_direction = COALESCE(EXCLUDED.xe80_percent_positive_direction,xe80_percent_positive_direction),
            -- xe80_percent_negative_direction = COALESCE(EXCLUDED.xe80_percent_negative_direction,xe80_percent_negative_direction),
            -- e80_per_day = COALESCE(EXCLUDED.e80_per_day,e80_per_day),
            -- e80_per_day_positive_direction = COALESCE(EXCLUDED.e80_per_day_positive_direction,e80_per_day_positive_direction),
            -- e80_per_day_negative_direction = COALESCE(EXCLUDED.e80_per_day_negative_direction,e80_per_day_negative_direction),
            -- e80_per_heavy_vehicle = COALESCE(EXCLUDED.e80_per_heavy_vehicle,e80_per_heavy_vehicle),
            -- e80_per_heavy_vehicle_positive_direction = COALESCE(EXCLUDED.e80_per_heavy_vehicle_positive_direction,e80_per_heavy_vehicle_positive_direction),
            -- e80_per_heavy_vehicle_negative_direction = COALESCE(EXCLUDED.e80_per_heavy_vehicle_negative_direction,e80_per_heavy_vehicle_negative_direction),
            -- worst_steering_single_axle_cnt = COALESCE(EXCLUDED.worst_steering_single_axle_cnt,worst_steering_single_axle_cnt),
            -- worst_steering_single_axle_olhv_perc = COALESCE(EXCLUDED.worst_steering_single_axle_olhv_perc,worst_steering_single_axle_olhv_perc),
            -- worst_steering_single_axle_tonperhv = COALESCE(EXCLUDED.worst_steering_single_axle_tonperhv,worst_steering_single_axle_tonperhv),
            -- worst_steering_double_axle_cnt = COALESCE(EXCLUDED.worst_steering_double_axle_cnt,worst_steering_double_axle_cnt),
            -- worst_steering_double_axle_olhv_perc = COALESCE(EXCLUDED.worst_steering_double_axle_olhv_perc,worst_steering_double_axle_olhv_perc),
            -- worst_steering_double_axle_tonperhv = COALESCE(EXCLUDED.worst_steering_double_axle_tonperhv,worst_steering_double_axle_tonperhv),
            -- worst_non_steering_single_axle_cnt = COALESCE(EXCLUDED.worst_non_steering_single_axle_cnt,worst_non_steering_single_axle_cnt),
            -- worst_non_steering_single_axle_olhv_perc = COALESCE(EXCLUDED.worst_non_steering_single_axle_olhv_perc,worst_non_steering_single_axle_olhv_perc),
            -- worst_non_steering_single_axle_tonperhv = COALESCE(EXCLUDED.worst_non_steering_single_axle_tonperhv,worst_non_steering_single_axle_tonperhv),
            -- worst_non_steering_double_axle_cnt = COALESCE(EXCLUDED.worst_non_steering_double_axle_cnt,worst_non_steering_double_axle_cnt),
            -- worst_non_steering_double_axle_olhv_perc = COALESCE(EXCLUDED.worst_non_steering_double_axle_olhv_perc,worst_non_steering_double_axle_olhv_perc),
            -- worst_non_steering_double_axle_tonperhv = COALESCE(EXCLUDED.worst_non_steering_double_axle_tonperhv,worst_non_steering_double_axle_tonperhv),
            -- worst_triple_axle_cnt = COALESCE(EXCLUDED.worst_triple_axle_cnt,worst_triple_axle_cnt),
            -- worst_triple_axle_olhv_perc = COALESCE(EXCLUDED.worst_triple_axle_olhv_perc,worst_triple_axle_olhv_perc),
            -- worst_triple_axle_tonperhv = COALESCE(EXCLUDED.worst_triple_axle_tonperhv,worst_triple_axle_tonperhv),
            -- bridge_formula_cnt = COALESCE(EXCLUDED.bridge_formula_cnt,bridge_formula_cnt),
            -- bridge_formula_olhv_perc = COALESCE(EXCLUDED.bridge_formula_olhv_perc,bridge_formula_olhv_perc),
            -- bridge_formula_tonperhv = COALESCE(EXCLUDED.bridge_formula_tonperhv,bridge_formula_tonperhv),
            -- gross_formula_cnt = COALESCE(EXCLUDED.gross_formula_cnt,gross_formula_cnt),
            -- gross_formula_olhv_perc = COALESCE(EXCLUDED.gross_formula_olhv_perc,gross_formula_olhv_perc),
            -- gross_formula_tonperhv = COALESCE(EXCLUDED.gross_formula_tonperhv,gross_formula_tonperhv),
            -- total_avg_cnt = COALESCE(EXCLUDED.total_avg_cnt,total_avg_cnt),
            -- total_avg_olhv_perc = COALESCE(EXCLUDED.total_avg_olhv_perc,total_avg_olhv_perc),
            -- total_avg_tonperhv = COALESCE(EXCLUDED.total_avg_tonperhv,total_avg_tonperhv),
            -- worst_steering_single_axle_cnt_positive_direciton = COALESCE(EXCLUDED.worst_steering_single_axle_cnt_positive_direciton,worst_steering_single_axle_cnt_positive_direciton),
            -- worst_steering_single_axle_olhv_perc_positive_direciton = COALESCE(EXCLUDED.worst_steering_single_axle_olhv_perc_positive_direciton,worst_steering_single_axle_olhv_perc_positive_direciton),
            -- worst_steering_single_axle_tonperhv_positive_direciton = COALESCE(EXCLUDED.worst_steering_single_axle_tonperhv_positive_direciton,worst_steering_single_axle_tonperhv_positive_direciton),
            -- worst_steering_double_axle_cnt_positive_direciton = COALESCE(EXCLUDED.worst_steering_double_axle_cnt_positive_direciton,worst_steering_double_axle_cnt_positive_direciton),
            -- worst_steering_double_axle_olhv_perc_positive_direciton = COALESCE(EXCLUDED.worst_steering_double_axle_olhv_perc_positive_direciton,worst_steering_double_axle_olhv_perc_positive_direciton),
            -- worst_steering_double_axle_tonperhv_positive_direciton = COALESCE(EXCLUDED.worst_steering_double_axle_tonperhv_positive_direciton,worst_steering_double_axle_tonperhv_positive_direciton),
            -- worst_non_steering_single_axle_cnt_positive_direciton = COALESCE(EXCLUDED.worst_non_steering_single_axle_cnt_positive_direciton,worst_non_steering_single_axle_cnt_positive_direciton),
            -- worst_non_steering_single_axle_olhv_perc_positive_direciton = COALESCE(EXCLUDED.worst_non_steering_single_axle_olhv_perc_positive_direciton,worst_non_steering_single_axle_olhv_perc_positive_direciton),
            -- worst_non_steering_single_axle_tonperhv_positive_direciton = COALESCE(EXCLUDED.worst_non_steering_single_axle_tonperhv_positive_direciton,worst_non_steering_single_axle_tonperhv_positive_direciton),
            -- worst_non_steering_double_axle_cnt_positive_direciton = COALESCE(EXCLUDED.worst_non_steering_double_axle_cnt_positive_direciton,worst_non_steering_double_axle_cnt_positive_direciton),
            -- worst_non_steering_double_axle_olhv_perc_positive_direciton = COALESCE(EXCLUDED.worst_non_steering_double_axle_olhv_perc_positive_direciton,worst_non_steering_double_axle_olhv_perc_positive_direciton),
            -- worst_non_steering_double_axle_tonperhv_positive_direciton = COALESCE(EXCLUDED.worst_non_steering_double_axle_tonperhv_positive_direciton,worst_non_steering_double_axle_tonperhv_positive_direciton),
            -- worst_triple_axle_cnt_positive_direciton = COALESCE(EXCLUDED.worst_triple_axle_cnt_positive_direciton,worst_triple_axle_cnt_positive_direciton),
            -- worst_triple_axle_olhv_perc_positive_direciton = COALESCE(EXCLUDED.worst_triple_axle_olhv_perc_positive_direciton,worst_triple_axle_olhv_perc_positive_direciton),
            -- worst_triple_axle_tonperhv_positive_direciton = COALESCE(EXCLUDED.worst_triple_axle_tonperhv_positive_direciton,worst_triple_axle_tonperhv_positive_direciton),
            -- bridge_formula_cnt_positive_direciton = COALESCE(EXCLUDED.bridge_formula_cnt_positive_direciton,bridge_formula_cnt_positive_direciton),
            -- bridge_formula_olhv_perc_positive_direciton = COALESCE(EXCLUDED.bridge_formula_olhv_perc_positive_direciton,bridge_formula_olhv_perc_positive_direciton),
            -- bridge_formula_tonperhv_positive_direciton = COALESCE(EXCLUDED.bridge_formula_tonperhv_positive_direciton,bridge_formula_tonperhv_positive_direciton),
            -- gross_formula_cnt_positive_direciton = COALESCE(EXCLUDED.gross_formula_cnt_positive_direciton,gross_formula_cnt_positive_direciton),
            -- gross_formula_olhv_perc_positive_direciton = COALESCE(EXCLUDED.gross_formula_olhv_perc_positive_direciton,gross_formula_olhv_perc_positive_direciton),
            -- gross_formula_tonperhv_positive_direciton = COALESCE(EXCLUDED.gross_formula_tonperhv_positive_direciton,gross_formula_tonperhv_positive_direciton),
            -- total_avg_cnt_positive_direciton = COALESCE(EXCLUDED.total_avg_cnt_positive_direciton,total_avg_cnt_positive_direciton),
            -- total_avg_olhv_perc_positive_direciton = COALESCE(EXCLUDED.total_avg_olhv_perc_positive_direciton,total_avg_olhv_perc_positive_direciton),
            -- total_avg_tonperhv_positive_direciton = COALESCE(EXCLUDED.total_avg_tonperhv_positive_direciton,total_avg_tonperhv_positive_direciton),
            -- worst_steering_single_axle_cnt_negative_direciton = COALESCE(EXCLUDED.worst_steering_single_axle_cnt_negative_direciton,worst_steering_single_axle_cnt_negative_direciton),
            -- worst_steering_single_axle_olhv_perc_negative_direciton = COALESCE(EXCLUDED.worst_steering_single_axle_olhv_perc_negative_direciton,worst_steering_single_axle_olhv_perc_negative_direciton),
            -- worst_steering_single_axle_tonperhv_negative_direciton = COALESCE(EXCLUDED.worst_steering_single_axle_tonperhv_negative_direciton,worst_steering_single_axle_tonperhv_negative_direciton),
            -- worst_steering_double_axle_cnt_negative_direciton = COALESCE(EXCLUDED.worst_steering_double_axle_cnt_negative_direciton,worst_steering_double_axle_cnt_negative_direciton),
            -- worst_steering_double_axle_olhv_perc_negative_direciton = COALESCE(EXCLUDED.worst_steering_double_axle_olhv_perc_negative_direciton,worst_steering_double_axle_olhv_perc_negative_direciton),
            -- worst_steering_double_axle_tonperhv_negative_direciton = COALESCE(EXCLUDED.worst_steering_double_axle_tonperhv_negative_direciton,worst_steering_double_axle_tonperhv_negative_direciton),
            -- worst_non_steering_single_axle_cnt_negative_direciton = COALESCE(EXCLUDED.worst_non_steering_single_axle_cnt_negative_direciton,worst_non_steering_single_axle_cnt_negative_direciton),
            -- worst_non_steering_single_axle_olhv_perc_negative_direciton = COALESCE(EXCLUDED.worst_non_steering_single_axle_olhv_perc_negative_direciton,worst_non_steering_single_axle_olhv_perc_negative_direciton),
            -- worst_non_steering_single_axle_tonperhv_negative_direciton = COALESCE(EXCLUDED.worst_non_steering_single_axle_tonperhv_negative_direciton,worst_non_steering_single_axle_tonperhv_negative_direciton),
            -- worst_non_steering_double_axle_cnt_negative_direciton = COALESCE(EXCLUDED.worst_non_steering_double_axle_cnt_negative_direciton,worst_non_steering_double_axle_cnt_negative_direciton),
            -- worst_non_steering_double_axle_olhv_perc_negative_direciton = COALESCE(EXCLUDED.worst_non_steering_double_axle_olhv_perc_negative_direciton,worst_non_steering_double_axle_olhv_perc_negative_direciton),
            -- worst_non_steering_double_axle_tonperhv_negative_direciton = COALESCE(EXCLUDED.worst_non_steering_double_axle_tonperhv_negative_direciton,worst_non_steering_double_axle_tonperhv_negative_direciton),
            -- worst_triple_axle_cnt_negative_direciton = COALESCE(EXCLUDED.worst_triple_axle_cnt_negative_direciton,worst_triple_axle_cnt_negative_direciton),
            -- worst_triple_axle_olhv_perc_negative_direciton = COALESCE(EXCLUDED.worst_triple_axle_olhv_perc_negative_direciton,worst_triple_axle_olhv_perc_negative_direciton),
            -- worst_triple_axle_tonperhv_negative_direciton = COALESCE(EXCLUDED.worst_triple_axle_tonperhv_negative_direciton,worst_triple_axle_tonperhv_negative_direciton),
            -- bridge_formula_cnt_negative_direciton = COALESCE(EXCLUDED.bridge_formula_cnt_negative_direciton,bridge_formula_cnt_negative_direciton),
            -- bridge_formula_olhv_perc_negative_direciton = COALESCE(EXCLUDED.bridge_formula_olhv_perc_negative_direciton,bridge_formula_olhv_perc_negative_direciton),
            -- bridge_formula_tonperhv_negative_direciton = COALESCE(EXCLUDED.bridge_formula_tonperhv_negative_direciton,bridge_formula_tonperhv_negative_direciton),
            -- gross_formula_cnt_negative_direciton = COALESCE(EXCLUDED.gross_formula_cnt_negative_direciton,gross_formula_cnt_negative_direciton),
            -- gross_formula_olhv_perc_negative_direciton = COALESCE(EXCLUDED.gross_formula_olhv_perc_negative_direciton,gross_formula_olhv_perc_negative_direciton),
            -- gross_formula_tonperhv_negative_direciton = COALESCE(EXCLUDED.gross_formula_tonperhv_negative_direciton,gross_formula_tonperhv_negative_direciton),
            -- total_avg_cnt_negative_direciton = COALESCE(EXCLUDED.total_avg_cnt_negative_direciton,total_avg_cnt_negative_direciton),
            -- total_avg_olhv_perc_negative_direciton = COALESCE(EXCLUDED.total_avg_olhv_perc_negative_direciton,total_avg_olhv_perc_negative_direciton),
            -- total_avg_tonperhv_negative_direciton = COALESCE(EXCLUDED.total_avg_tonperhv_negative_direciton,total_avg_tonperhv_negative_direciton),
            -- egrl_percent_positive_direction = COALESCE(EXCLUDED.egrl_percent_positive_direction,egrl_percent_positive_direction),
            -- egrl_percent_negative_direction = COALESCE(EXCLUDED.egrl_percent_negative_direction,egrl_percent_negative_direction),
            -- egrw_percent_positive_direction = COALESCE(EXCLUDED.egrw_percent_positive_direction,egrw_percent_positive_direction),
            -- egrw_percent_negative_direction = COALESCE(EXCLUDED.egrw_percent_negative_direction,egrw_percent_negative_direction),
            -- num_weighed = COALESCE(EXCLUDED.num_weighed,num_weighed),
            -- num_weighed_positive_direction = COALESCE(EXCLUDED.num_weighed_positive_direction,num_weighed_positive_direction),
            -- num_weighed_negative_direction = COALESCE(EXCLUDED.num_weighed_negative_direction,num_weighed_negative_direction),
            -- wst_2_axle_busses_cnt_pos_dir = COALESCE(EXCLUDED.wst_2_axle_busses_cnt_pos_dir,wst_2_axle_busses_cnt_pos_dir),
            -- wst_2_axle_6_tyre_single_units_cnt_pos_dir = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_cnt_pos_dir,wst_2_axle_6_tyre_single_units_cnt_pos_dir),
            -- wst_busses_with_3_or_4_axles_cnt_pos_dir = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_cnt_pos_dir,wst_busses_with_3_or_4_axles_cnt_pos_dir),
            -- wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_pos_dir = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_pos_dir,wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_pos_dir),
            -- wst_3_axle_su_incl_single_axle_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_cnt_pos_dir,wst_3_axle_su_incl_single_axle_trailer_cnt_pos_dir),
            -- wst_4_or_less_axle_incl_a_single_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_cnt_pos_dir,wst_4_or_less_axle_incl_a_single_trailer_cnt_pos_dir),
            -- wst_busses_with_5_or_more_axles_cnt_pos_dir = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_cnt_pos_dir,wst_busses_with_5_or_more_axles_cnt_pos_dir),
            -- wst_3_axle_su_and_trailer_more_than_4_axles_cnt_pos_dir = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_cnt_pos_dir,wst_3_axle_su_and_trailer_more_than_4_axles_cnt_pos_dir),
            -- wst_5_axle_single_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_5_axle_single_trailer_cnt_pos_dir,wst_5_axle_single_trailer_cnt_pos_dir),
            -- wst_6_axle_single_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_6_axle_single_trailer_cnt_pos_dir,wst_6_axle_single_trailer_cnt_pos_dir),
            -- wst_5_or_less_axle_multi_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_cnt_pos_dir,wst_5_or_less_axle_multi_trailer_cnt_pos_dir),
            -- wst_6_axle_multi_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_cnt_pos_dir,wst_6_axle_multi_trailer_cnt_pos_dir),
            -- wst_7_axle_multi_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_cnt_pos_dir,wst_7_axle_multi_trailer_cnt_pos_dir),
            -- wst_8_or_more_axle_multi_trailer_cnt_pos_dir = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_cnt_pos_dir,wst_8_or_more_axle_multi_trailer_cnt_pos_dir),
            -- wst_2_axle_busses_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_2_axle_busses_olhv_perc_pos_dir,wst_2_axle_busses_olhv_perc_pos_dir),
            -- wst_2_axle_6_tyre_single_units_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_olhv_perc_pos_dir,wst_2_axle_6_tyre_single_units_olhv_perc_pos_dir),
            -- wst_busses_with_3_or_4_axles_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_olhv_perc_pos_dir,wst_busses_with_3_or_4_axles_olhv_perc_pos_dir),
            -- wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_pos_dir,wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_pos_dir),
            -- wst_3_axle_su_incl_single_axle_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_olhv_perc_pos_dir,wst_3_axle_su_incl_single_axle_trailer_olhv_perc_pos_dir),
            -- wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_pos_dir,wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_pos_dir),
            -- wst_busses_with_5_or_more_axles_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_olhv_perc_pos_dir,wst_busses_with_5_or_more_axles_olhv_perc_pos_dir),
            -- wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_pos_dir,wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_pos_dir),
            -- wst_5_axle_single_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_5_axle_single_trailer_olhv_perc_pos_dir,wst_5_axle_single_trailer_olhv_perc_pos_dir),
            -- wst_6_axle_single_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_6_axle_single_trailer_olhv_perc_pos_dir,wst_6_axle_single_trailer_olhv_perc_pos_dir),
            -- wst_5_or_less_axle_multi_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_olhv_perc_pos_dir,wst_5_or_less_axle_multi_trailer_olhv_perc_pos_dir),
            -- wst_6_axle_multi_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_olhv_perc_pos_dir,wst_6_axle_multi_trailer_olhv_perc_pos_dir),
            -- wst_7_axle_multi_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_olhv_perc_pos_dir,wst_7_axle_multi_trailer_olhv_perc_pos_dir),
            -- wst_8_or_more_axle_multi_trailer_olhv_perc_pos_dir = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_olhv_perc_pos_dir,wst_8_or_more_axle_multi_trailer_olhv_perc_pos_dir),
            -- wst_2_axle_busses_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_2_axle_busses_tonperhv_pos_dir,wst_2_axle_busses_tonperhv_pos_dir),
            -- wst_2_axle_6_tyre_single_units_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_tonperhv_pos_dir,wst_2_axle_6_tyre_single_units_tonperhv_pos_dir),
            -- wst_busses_with_3_or_4_axles_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_tonperhv_pos_dir,wst_busses_with_3_or_4_axles_tonperhv_pos_dir),
            -- wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_pos_dir,wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_pos_dir),
            -- wst_3_axle_su_incl_single_axle_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_tonperhv_pos_dir,wst_3_axle_su_incl_single_axle_trailer_tonperhv_pos_dir),
            -- wst_4_or_less_axle_incl_a_single_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_tonperhv_pos_dir,wst_4_or_less_axle_incl_a_single_trailer_tonperhv_pos_dir),
            -- wst_busses_with_5_or_more_axles_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_tonperhv_pos_dir,wst_busses_with_5_or_more_axles_tonperhv_pos_dir),
            -- wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_pos_dir,wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_pos_dir),
            -- wst_5_axle_single_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_5_axle_single_trailer_tonperhv_pos_dir,wst_5_axle_single_trailer_tonperhv_pos_dir),
            -- wst_6_axle_single_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_6_axle_single_trailer_tonperhv_pos_dir,wst_6_axle_single_trailer_tonperhv_pos_dir),
            -- wst_5_or_less_axle_multi_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_tonperhv_pos_dir,wst_5_or_less_axle_multi_trailer_tonperhv_pos_dir),
            -- wst_6_axle_multi_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_tonperhv_pos_dir,wst_6_axle_multi_trailer_tonperhv_pos_dir),
            -- wst_7_axle_multi_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_tonperhv_pos_dir,wst_7_axle_multi_trailer_tonperhv_pos_dir),
            -- wst_8_or_more_axle_multi_trailer_tonperhv_pos_dir = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_tonperhv_pos_dir,wst_8_or_more_axle_multi_trailer_tonperhv_pos_dir),
            -- wst_2_axle_busses_cnt_neg_dir = COALESCE(EXCLUDED.wst_2_axle_busses_cnt_neg_dir,wst_2_axle_busses_cnt_neg_dir),
            -- wst_2_axle_6_tyre_single_units_cnt_neg_dir = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_cnt_neg_dir,wst_2_axle_6_tyre_single_units_cnt_neg_dir),
            -- wst_busses_with_3_or_4_axles_cnt_neg_dir = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_cnt_neg_dir,wst_busses_with_3_or_4_axles_cnt_neg_dir),
            -- wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_neg_dir = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_neg_dir,wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt_neg_dir),
            -- wst_3_axle_su_incl_single_axle_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_cnt_neg_dir,wst_3_axle_su_incl_single_axle_trailer_cnt_neg_dir),
            -- wst_4_or_less_axle_incl_a_single_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_cnt_neg_dir,wst_4_or_less_axle_incl_a_single_trailer_cnt_neg_dir),
            -- wst_busses_with_5_or_more_axles_cnt_neg_dir = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_cnt_neg_dir,wst_busses_with_5_or_more_axles_cnt_neg_dir),
            -- wst_3_axle_su_and_trailer_more_than_4_axles_cnt_neg_dir = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_cnt_neg_dir,wst_3_axle_su_and_trailer_more_than_4_axles_cnt_neg_dir),
            -- wst_5_axle_single_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_5_axle_single_trailer_cnt_neg_dir,wst_5_axle_single_trailer_cnt_neg_dir),
            -- wst_6_axle_single_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_6_axle_single_trailer_cnt_neg_dir,wst_6_axle_single_trailer_cnt_neg_dir),
            -- wst_5_or_less_axle_multi_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_cnt_neg_dir,wst_5_or_less_axle_multi_trailer_cnt_neg_dir),
            -- wst_6_axle_multi_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_cnt_neg_dir,wst_6_axle_multi_trailer_cnt_neg_dir),
            -- wst_7_axle_multi_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_cnt_neg_dir,wst_7_axle_multi_trailer_cnt_neg_dir),
            -- wst_8_or_more_axle_multi_trailer_cnt_neg_dir = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_cnt_neg_dir,wst_8_or_more_axle_multi_trailer_cnt_neg_dir),
            -- wst_2_axle_busses_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_2_axle_busses_olhv_perc_neg_dir,wst_2_axle_busses_olhv_perc_neg_dir),
            -- wst_2_axle_6_tyre_single_units_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_olhv_perc_neg_dir,wst_2_axle_6_tyre_single_units_olhv_perc_neg_dir),
            -- wst_busses_with_3_or_4_axles_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_olhv_perc_neg_dir,wst_busses_with_3_or_4_axles_olhv_perc_neg_dir),
            -- wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_neg_dir,wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc_neg_dir),
            -- wst_3_axle_su_incl_single_axle_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_olhv_perc_neg_dir,wst_3_axle_su_incl_single_axle_trailer_olhv_perc_neg_dir),
            -- wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_neg_dir,wst_4_or_less_axle_incl_a_single_trailer_olhv_perc_neg_dir),
            -- wst_busses_with_5_or_more_axles_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_olhv_perc_neg_dir,wst_busses_with_5_or_more_axles_olhv_perc_neg_dir),
            -- wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_neg_dir,wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc_neg_dir),
            -- wst_5_axle_single_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_5_axle_single_trailer_olhv_perc_neg_dir,wst_5_axle_single_trailer_olhv_perc_neg_dir),
            -- wst_6_axle_single_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_6_axle_single_trailer_olhv_perc_neg_dir,wst_6_axle_single_trailer_olhv_perc_neg_dir),
            -- wst_5_or_less_axle_multi_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_olhv_perc_neg_dir,wst_5_or_less_axle_multi_trailer_olhv_perc_neg_dir),
            -- wst_6_axle_multi_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_olhv_perc_neg_dir,wst_6_axle_multi_trailer_olhv_perc_neg_dir),
            -- wst_7_axle_multi_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_olhv_perc_neg_dir,wst_7_axle_multi_trailer_olhv_perc_neg_dir),
            -- wst_8_or_more_axle_multi_trailer_olhv_perc_neg_dir = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_olhv_perc_neg_dir,wst_8_or_more_axle_multi_trailer_olhv_perc_neg_dir),
            -- wst_2_axle_busses_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_2_axle_busses_tonperhv_neg_dir,wst_2_axle_busses_tonperhv_neg_dir),
            -- wst_2_axle_6_tyre_single_units_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_tonperhv_neg_dir,wst_2_axle_6_tyre_single_units_tonperhv_neg_dir),
            -- wst_busses_with_3_or_4_axles_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_tonperhv_neg_dir,wst_busses_with_3_or_4_axles_tonperhv_neg_dir),
            -- wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_neg_dir,wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv_neg_dir),
            -- wst_3_axle_su_incl_single_axle_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_tonperhv_neg_dir,wst_3_axle_su_incl_single_axle_trailer_tonperhv_neg_dir),
            -- wst_4_or_less_axle_incl_a_single_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_tonperhv_neg_dir,wst_4_or_less_axle_incl_a_single_trailer_tonperhv_neg_dir),
            -- wst_busses_with_5_or_more_axles_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_tonperhv_neg_dir,wst_busses_with_5_or_more_axles_tonperhv_neg_dir),
            -- wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_neg_dir,wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv_neg_dir),
            -- wst_5_axle_single_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_5_axle_single_trailer_tonperhv_neg_dir,wst_5_axle_single_trailer_tonperhv_neg_dir),
            -- wst_6_axle_single_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_6_axle_single_trailer_tonperhv_neg_dir,wst_6_axle_single_trailer_tonperhv_neg_dir),
            -- wst_5_or_less_axle_multi_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_tonperhv_neg_dir,wst_5_or_less_axle_multi_trailer_tonperhv_neg_dir),
            -- wst_6_axle_multi_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_tonperhv_neg_dir,wst_6_axle_multi_trailer_tonperhv_neg_dir),
            -- wst_7_axle_multi_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_tonperhv_neg_dir,wst_7_axle_multi_trailer_tonperhv_neg_dir),
            -- wst_8_or_more_axle_multi_trailer_tonperhv_neg_dir = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_tonperhv_neg_dir,wst_8_or_more_axle_multi_trailer_tonperhv_neg_dir),
            -- wst_2_axle_busses_cnt = COALESCE(EXCLUDED.wst_2_axle_busses_cnt,wst_2_axle_busses_cnt),
            -- wst_2_axle_6_tyre_single_units_cnt = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_cnt,wst_2_axle_6_tyre_single_units_cnt),
            -- wst_busses_with_3_or_4_axles_cnt = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_cnt,wst_busses_with_3_or_4_axles_cnt),
            -- wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt,wst_2_axle_6tyre_su_with_trailer_4axles_max_cnt),
            -- wst_3_axle_su_incl_single_axle_trailer_cnt = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_cnt,wst_3_axle_su_incl_single_axle_trailer_cnt),
            -- wst_4_or_less_axle_incl_a_single_trailer_cnt = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_cnt,wst_4_or_less_axle_incl_a_single_trailer_cnt),
            -- wst_busses_with_5_or_more_axles_cnt = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_cnt,wst_busses_with_5_or_more_axles_cnt),
            -- wst_3_axle_su_and_trailer_more_than_4_axles_cnt = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_cnt,wst_3_axle_su_and_trailer_more_than_4_axles_cnt),
            -- wst_5_axle_single_trailer_cnt = COALESCE(EXCLUDED.wst_5_axle_single_trailer_cnt,wst_5_axle_single_trailer_cnt),
            -- wst_6_axle_single_trailer_cnt = COALESCE(EXCLUDED.wst_6_axle_single_trailer_cnt,wst_6_axle_single_trailer_cnt),
            -- wst_5_or_less_axle_multi_trailer_cnt = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_cnt,wst_5_or_less_axle_multi_trailer_cnt),
            -- wst_6_axle_multi_trailer_cnt = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_cnt,wst_6_axle_multi_trailer_cnt),
            -- wst_7_axle_multi_trailer_cnt = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_cnt,wst_7_axle_multi_trailer_cnt),
            -- wst_8_or_more_axle_multi_trailer_cnt = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_cnt,wst_8_or_more_axle_multi_trailer_cnt),
            -- wst_2_axle_busses_olhv_perc = COALESCE(EXCLUDED.wst_2_axle_busses_olhv_perc,wst_2_axle_busses_olhv_perc),
            -- wst_2_axle_6_tyre_single_units_olhv_perc = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_olhv_perc,wst_2_axle_6_tyre_single_units_olhv_perc),
            -- wst_busses_with_3_or_4_axles_olhv_perc = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_olhv_perc,wst_busses_with_3_or_4_axles_olhv_perc),
            -- wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc,wst_2_axle_6tyre_su_with_trailer_4axles_max_olhv_perc),
            -- wst_3_axle_su_incl_single_axle_trailer_olhv_perc = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_olhv_perc,wst_3_axle_su_incl_single_axle_trailer_olhv_perc),
            -- wst_4_or_less_axle_incl_a_single_trailer_olhv_perc = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_olhv_perc,wst_4_or_less_axle_incl_a_single_trailer_olhv_perc),
            -- wst_busses_with_5_or_more_axles_olhv_perc = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_olhv_perc,wst_busses_with_5_or_more_axles_olhv_perc),
            -- wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc,wst_3_axle_su_and_trailer_more_than_4_axles_olhv_perc),
            -- wst_5_axle_single_trailer_olhv_perc = COALESCE(EXCLUDED.wst_5_axle_single_trailer_olhv_perc,wst_5_axle_single_trailer_olhv_perc),
            -- wst_6_axle_single_trailer_olhv_perc = COALESCE(EXCLUDED.wst_6_axle_single_trailer_olhv_perc,wst_6_axle_single_trailer_olhv_perc),
            -- wst_5_or_less_axle_multi_trailer_olhv_perc = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_olhv_perc,wst_5_or_less_axle_multi_trailer_olhv_perc),
            -- wst_6_axle_multi_trailer_olhv_perc = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_olhv_perc,wst_6_axle_multi_trailer_olhv_perc),
            -- wst_7_axle_multi_trailer_olhv_perc = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_olhv_perc,wst_7_axle_multi_trailer_olhv_perc),
            -- wst_8_or_more_axle_multi_trailer_olhv_perc = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_olhv_perc,wst_8_or_more_axle_multi_trailer_olhv_perc),
            -- wst_2_axle_busses_tonperhv = COALESCE(EXCLUDED.wst_2_axle_busses_tonperhv,wst_2_axle_busses_tonperhv),
            -- wst_2_axle_6_tyre_single_units_tonperhv = COALESCE(EXCLUDED.wst_2_axle_6_tyre_single_units_tonperhv,wst_2_axle_6_tyre_single_units_tonperhv),
            -- wst_busses_with_3_or_4_axles_tonperhv = COALESCE(EXCLUDED.wst_busses_with_3_or_4_axles_tonperhv,wst_busses_with_3_or_4_axles_tonperhv),
            -- wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv = COALESCE(EXCLUDED.wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv,wst_2_axle_6tyre_su_with_trailer_4axles_max_tonperhv),
            -- wst_3_axle_su_incl_single_axle_trailer_tonperhv = COALESCE(EXCLUDED.wst_3_axle_su_incl_single_axle_trailer_tonperhv,wst_3_axle_su_incl_single_axle_trailer_tonperhv),
            -- wst_4_or_less_axle_incl_a_single_trailer_tonperhv = COALESCE(EXCLUDED.wst_4_or_less_axle_incl_a_single_trailer_tonperhv,wst_4_or_less_axle_incl_a_single_trailer_tonperhv),
            -- wst_busses_with_5_or_more_axles_tonperhv = COALESCE(EXCLUDED.wst_busses_with_5_or_more_axles_tonperhv,wst_busses_with_5_or_more_axles_tonperhv),
            -- wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv = COALESCE(EXCLUDED.wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv,wst_3_axle_su_and_trailer_more_than_4_axles_tonperhv),
            -- wst_5_axle_single_trailer_tonperhv = COALESCE(EXCLUDED.wst_5_axle_single_trailer_tonperhv,wst_5_axle_single_trailer_tonperhv),
            -- wst_6_axle_single_trailer_tonperhv = COALESCE(EXCLUDED.wst_6_axle_single_trailer_tonperhv,wst_6_axle_single_trailer_tonperhv),
            -- wst_5_or_less_axle_multi_trailer_tonperhv = COALESCE(EXCLUDED.wst_5_or_less_axle_multi_trailer_tonperhv,wst_5_or_less_axle_multi_trailer_tonperhv),
            -- wst_6_axle_multi_trailer_tonperhv = COALESCE(EXCLUDED.wst_6_axle_multi_trailer_tonperhv,wst_6_axle_multi_trailer_tonperhv),
            -- wst_7_axle_multi_trailer_tonperhv = COALESCE(EXCLUDED.wst_7_axle_multi_trailer_tonperhv,wst_7_axle_multi_trailer_tonperhv),
            -- wst_8_or_more_axle_multi_trailer_tonperhv = COALESCE(EXCLUDED.wst_8_or_more_axle_multi_trailer_tonperhv,wst_8_or_more_axle_multi_trailer_tonperhv)
            ;
        """
        return UPSERT_STRING

    def get_headers_to_update(self) -> pd.DataFrame:
        """
        It fetches a list of header ids from a database table and returns them as a pandas dataframe
        :return: A dataframe with the header_ids
        """
        print('fetching headers to update')
        header_ids = pd.read_sql_query(q.GET_HSWIM_HEADER_IDS, config.ENGINE)
        return header_ids

    def wim_header_upsert_func1(self, header_id: str) -> str:
        """
        It returns a tuple of three strings, each of which is a SQL query.

        :param header_id: str = '1'
        :type header_id: str
        :return: a tuple of 3 strings.
        """
        SELECT_TYPE10_QRY = f"""SELECT * FROM trafc.electronic_count_data_type_10 t10
            left join traf_lu.vehicle_classes_scheme08 c on c.id = t10.vehicle_class_code_primary_scheme
            where t10.header_id = '{header_id}'
            """
        AXLE_SPACING_SELECT_QRY = f"""SELECT 
             t10.id,
            t10.header_id, 
            t10.start_datetime,
            t10.edit_code,
            t10.vehicle_class_code_primary_scheme, 
            t10.vehicle_class_code_secondary_scheme,
            t10.direction,
            t10.axle_count,
            axs.axle_spacing_number,
            axs.axle_spacing_cm,
            wm.wheel_mass_number,
            wm.wheel_mass,
            vm.kg as vehicle_mass_limit_kg,
            sum(wm.wheel_mass) over(partition by t10.id) as gross_mass
            FROM trafc.electronic_count_data_type_10 t10
            left join trafc.traffic_e_type10_wheel_mass wm ON wm.type10_id = t10.data_id
            left join trafc.traffic_e_type10_axle_spacing axs ON axs.type10_id = t10.data_id and axs.axle_spacing_number = wm.wheel_mass_number
            Left join traf_lu.gross_vehicle_mass_limits vm on vm.number_of_axles = t10.axle_count
            where t10.header_id = '{header_id}' and wm.wheel_mass_number is not null
            """
        WHEEL_MASS_SELECT_QRY = f"""SELECT 
            t10.id,
            t10.header_id, 
            t10.start_datetime,
            t10.edit_code,
            t10.vehicle_class_code_primary_scheme, 
            t10.vehicle_class_code_secondary_scheme,
            t10.direction,
            t10.axle_count,
            wm.wheel_mass_number,
            wm.wheel_mass,
            vm.kg as vehicle_mass_limit_kg,
            sum(wm.wheel_mass) over(partition by t10.id) as gross_mass
            FROM trafc.electronic_count_data_type_10 t10
            inner join trafc.traffic_e_type10_wheel_mass wm ON wm.type10_id = t10.data_id
            inner join traf_lu.gross_vehicle_mass_limits vm on vm.number_of_axles = t10.axle_count
            where t10.header_id = '{header_id}'
            """
        return SELECT_TYPE10_QRY, AXLE_SPACING_SELECT_QRY, WHEEL_MASS_SELECT_QRY

    def wim_header_upsert_func2(self, SELECT_TYPE10_QRY: str, AXLE_SPACING_SELECT_QRY: str, WHEEL_MASS_SELECT_QRY: str) -> pd.DataFrame:
        """
        This function takes in three SQL queries and returns three dataframes

        :param SELECT_TYPE10_QRY: str = "SELECT * FROM trafc.electronic_count_data_type_10 t10 limit 1"
        :type SELECT_TYPE10_QRY: str
        :param AXLE_SPACING_SELECT_QRY: str = "SELECT * FROM axle_spacinglimit 1"
        :type AXLE_SPACING_SELECT_QRY: str
        :param WHEEL_MASS_SELECT_QRY: str = "select * from wheel_masslimit 1"
        :type WHEEL_MASS_SELECT_QRY: str
        :return: A tuple of 3 dataframes
        """
        df = pd.read_sql_query(SELECT_TYPE10_QRY, config.ENGINE)
        df = df.fillna(0)
        df2 = pd.read_sql_query(AXLE_SPACING_SELECT_QRY, config.ENGINE)
        df2 = df2.fillna(0)
        df3 = pd.read_sql_query(WHEEL_MASS_SELECT_QRY, config.ENGINE)
        df3 = df3.fillna(0)
        return df, df2, df3

    def update_existing(self, header_ids: pd.DataFrame) -> None:
        """
        It takes a list of header_ids, and for each header_id, it runs a series of functions that calculate
        values for the header_id, and then upserts the calculated values into the database
        """
        for header_id in list(header_ids['header_id'].astype(str)):
            if header_id is None:
                pass
            else:
                SELECT_TYPE10_QRY, AXLE_SPACING_SELECT_QRY, WHEEL_MASS_SELECT_QRY = self.wim_header_upsert_func1(
                    header_id)
                df, df2, df3 = self.wim_header_upsert_func2(
                    SELECT_TYPE10_QRY, AXLE_SPACING_SELECT_QRY, WHEEL_MASS_SELECT_QRY)
                if df2 is None or df3 is None:
                    print(f"passing {header_id}")
                else:
                    print(f'working on {header_id}')
                    self.wim_header_calcs(df, df2, df3)
                    insert_string = self.wim_header_upsert(header_id)
                    with config.ENGINE.connect() as conn:
                        print(f'upserting: {header_id}')
                        conn.execute(insert_string)
                        print('COMPLETE')

    def main(self):
        """
        It takes a dataframe, splits it into two dataframes, then pushes the first dataframe to a
        database table, and the second dataframe to a number of other database tables
        """
        try:
            if self.data_df is not None and self.header_id is not None:
                pass
            else:
                data, sub_data = self.type_10()
                data = data[data.columns.intersection(self.t10_cols)]
                main.push_to_db(data, config.TYPE_10_TBL_NAME)

                self.sub_data = sub_data.replace(r'^\s*$', np.NaN, regex=True)
                self.sub_data = sub_data.drop("index", axis=1, errors='ignore')
                self.wx_data = sub_data.loc[sub_data['sub_data_type_code'].str.lower(
                ).str[0] == 'w']
                self.sx_data = sub_data.loc[sub_data['sub_data_type_code'].str.lower(
                ).str[0] == 's']
                self.gx_data = sub_data.loc[sub_data['sub_data_type_code'].str.lower(
                ).str[0] == 'g']
                self.vx_data = sub_data.loc[sub_data['sub_data_type_code'].str.lower(
                ).str[0] == 'v']
                self.tx_data = sub_data.loc[sub_data['sub_data_type_code'].str.lower(
                ).str[0] == 't']
                self.ax_data = sub_data.loc[sub_data['sub_data_type_code'].str.lower(
                ).str[0] == 'a']
                self.cx_data = sub_data.loc[sub_data['sub_data_type_code'].str.lower(
                ).str[0] == 'c']

                if self.wx_data.empty:
                    pass
                else:
                    self.wx_data.rename(columns={
                                        "value": "wheel_mass", "number": "wheel_mass_number", "id": "type10_id"}, inplace=True)
                    main.push_to_db(self.wx_data, config.WX_TABLE)

                if self.ax_data.empty:
                    pass
                else:
                    main.push_to_db(self.ax_data, config.AX_TABLE)

                if self.gx_data.empty:
                    pass
                else:
                    main.push_to_db(self.gx_data, config.GX_TABLE)

                if self.sx_data.empty:
                    pass
                else:
                    self.sx_data.rename(columns={
                                        "value": "axle_spacing_cm", "number": "axle_spacing_number", "id": "type10_id"}, inplace=True)
                    self.sx_data = self.sx_data.drop(
                        ["offset_sensor_detection_code", "mass_measurement_resolution_kg"], axis=1)
                    main.push_to_db(self.sx_data, config.SX_TABLE)

                if self.tx_data.empty:
                    pass
                else:
                    self.tx_data.rename(columns={
                                        "value": "tyre_code", "number": "tyre_number", "id": "type10_id"}, inplace=True)
                    self.tx_data = self.tx_data.drop(
                        ["offset_sensor_detection_code", "mass_measurement_resolution_kg"], axis=1)
                    main.push_to_db(self.tx_data, config.TX_TABLE)

                if self.cx_data.empty:
                    pass
                else:
                    main.push_to_db(self.cx_data, config.CX_TABLE)

                if self.vx_data.empty:
                    pass
                else:
                    self.vx_data.rename(columns={"value": "group_axle_count", "offset_sensor_detection_code":
                                        "vehicle_registration_number", "number": "group_axle_number", "id": "type10_id"}, inplace=True)
                    self.vx_data = self.vx_data.drop(
                        ["mass_measurement_resolution_kg"], axis=1)
                    main.push_to_db(self.vx_data, config.VX_TABLE)
        except Exception:
            traceback.print_exc()


if __name__ == '__main__':
    WIM = Wim(None, None, None, None, None)
    header_ids = WIM.header_ids
    print(header_ids)
    WIM.update_existing(header_ids)
    # SELECT_TYPE10_QRY, AXLE_SPACING_SELECT_QRY, WHEEL_MASS_SELECT_QRY = WIM.wim_header_upsert_func1(
    #     'bba9b8bf-9db6-4970-95d3-80f72393af99')
    # df, df2, df3 = WIM.wim_header_upsert_func2(
    #     SELECT_TYPE10_QRY, AXLE_SPACING_SELECT_QRY, WHEEL_MASS_SELECT_QRY)
    # WIM.wim_header_calcs(df, df2, df3)
    # upsert = WIM.wim_header_upsert('bba9b8bf-9db6-4970-95d3-80f72393af99')
    # print(upsert)
