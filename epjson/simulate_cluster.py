import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import logging
import pandas as pd
import numpy as np
import datetime
from tqdm import tqdm

from epjson.epjson_idf import EpJsonIDF

logging.basicConfig()
logger = logging.getLogger("BATCH")
logger.setLevel(logging.INFO)

umi_weight_map = {
    "Perim": "PerimeterAreaWeight",
    "Core": "CoreAreaWeight"
}

# selected_csv_outputs = [
#     "%ZONE%:Zone Lights Electric Energy [J](Hourly)",
#     "%ZONE%:Zone Electric Equipment Electric Energy [J](Hourly)",
#     "%ZONE% IDEAL LOADS AIR:Zone Ideal Loads Supply Air Total Heating Energy [J](Hourly)",
#     "%ZONE% IDEAL LOADS AIR:Zone Ideal Loads Supply Air Total Cooling Energy [J](Hourly)",
#     "DHW %ZONE%:Water Use Equipment Heating Energy [J](Hourly)"
#     ]

selected_outputs = [
    "Zone Lights Electricity Energy",
    "Electric Equipment Electricity Energy",
    "Zone Ideal Loads Supply Air Total Heating Energy",
    "Zone Ideal Loads Supply Air Total Cooling Energy",
    "Water Use Equipment Heating Energy"
    ]

class SimulateCluster:
    def __init__(
            self,
            idf_list,
            epw,
            weights_df,
            weight_map=umi_weight_map,
            idf_path_col = "ShoeboxPath",
            building_col = "ParentBuildingId",
            eplus_location = Path("C:\EnergyPlusV22-2-0"),
    ):
        self.idfs = idf_list
        self.eplus_location = Path(eplus_location)
        self.epw = epw
        if idf_path_col != "ShoeboxPath":
            weights_df.rename(columns={idf_path_col: "ShoeboxPath"})
        self.building_col = building_col
        self.weights = weights_df
        self.weight_map = weight_map

        self.make_epjsons()
        self.get_areas()

    def make_epjsons(self):
        epjsons = {}
        unique_df = self.weights.groupby("ShoeboxPath").first().reset_index()
        for _, row in tqdm(unique_df.iterrows(), total=unique_df.shape[0], desc="Fetching epjsons..."):
            try:
                epjson = EpJsonIDF.from_idf(row.ShoeboxPath)
                epjsons[row.ShoeboxPath] = epjson
            except Exception as e:
                logger.error(f"ERROR FOR EPJSON {row.ShoeboxPath}")
                raise e
        self.epjsons = epjsons

    def get_areas(self):
        for zone in self.weight_map.keys():
            self.weights[f"{zone}_Area"] = 0
        unique_df = self.weights.groupby("ShoeboxPath").first().reset_index()
        for _, row in unique_df.iterrows():
            # print(epjson.zone_values("Lights", "watts_per_zone_floor_area"))
            epjson = self.epjsons[row.ShoeboxPath]
            areas = epjson.calculate_zone_areas()
            for zone in self.weight_map.keys():
                self.weights.loc[self.weights.ShoeboxPath == row.ShoeboxPath, f"{zone}_Area"] = areas[zone]
        # Get total gross building area from umi
        #TODO
        # self.weights["TotalFloorArea"] = self.weights["PerimeterArea"] + self.weights["CoreArea"] 
        self.weights["TotalFloorArea"] = self.weights["bldg_perim_area"] + self.weights["bldg_core_area"] 

    def fetch_building_results_parallel(self, buildings, max_workers=6):
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            r = list(tqdm(executor.map(self.fetch_building_results, buildings), total=len(buildings), desc="Fetching building results"))
        results = {x[0]:x[1] for x in r}
        # warnings = [x[1] for x in r]
        self.energy_results = results
        return results

    def fetch_building_results(self, building_id, selected_outputs=selected_outputs):
        results_df = pd.DataFrame(columns=selected_outputs, index=range(8760), data= np.zeros((8760, len(selected_outputs)))) #TODO: might not be hourly
        logger.info(f"Fetching results for {building_id}")
        df = self.weights[self.weights[self.building_col] == building_id]
        logger.info(f"There are {df.shape[0]} shoeboxes for {building_id}")

        zones = [x for x in self.weight_map.keys()]
        for _, row in df.iterrows():
            try:
                sb_results = EpJsonIDF.process_results(row["ShoeboxPath"], selected_outputs)
                sb_results.columns = sb_results.columns.droplevel()
                
                for zone in zones:
                    cols = [x for x in sb_results.columns if zone.upper() in x[0]]
                    area = row[f"{zone}_Area"]
                    norm_results = sb_results.loc[:, cols]
                    # norm_results = sb_results.loc[:, cols].div(area) # TODO
                    sb_results.loc[:, cols] = norm_results.mul(row[self.weight_map[zone]]).values
                    sb_results.reset_index(drop=True, inplace=True)
                results_df.loc[:, :] = sb_results.groupby(level=1, axis=1).sum() + results_df
            except Exception as e:
                logger.error(f"Problem with shoebox {row.ShoeboxPath}")
                return (building_id, "ERROR")
                # logger.error(e)
                # raise e
                # results_df.loc[:, :] += new
        norm_cols = [x+"_norm" for x in results_df.columns]
        building_area = list(df.TotalFloorArea)[0] * list(df.floor_count)[0] #TODO
        results_df.loc[:, norm_cols] = results_df.div(building_area).values
        results_df = results_df.mul(2.77e-7)
        results_df["ModelArea"] = building_area
        results_df["DateTime"] = pd.date_range(start=datetime.datetime(year=2018, day=1, month=1, hour=0), periods=8760, freq="H")
        results_df["BuildingId"] = building_id
        return (building_id, results_df)
    
    def _simulate_single_idf(self, idf_path, override=True):
        if not os.path.isfile(Path(idf_path).parent / "eplusout.sql"):
            EpJsonIDF.run(idf_path=idf_path, eplus_location=self.eplus_location, epw=self.epw)
            # assert os.path.isfile(Path(idf_path).parent / "eplusout.sql"), f"Error with getting SQL for {idf_path}"
        elif override:
            EpJsonIDF.run(idf_path=idf_path, eplus_location=self.eplus_location, epw=self.epw)
        return (Path(idf_path).name, EpJsonIDF.error_report(idf_path)[0])

    def parallel_simulate(self, max_workers=6):
        # TODO: enable output directory, save idfs in files where they stop matching
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            r = list(tqdm(executor.map(self._simulate_single_idf, self.idfs), total=len(self.idfs)))
        logger.info("Simulated all idfs.")
        errors = {x[0]:x[1] for x in r if len(x[1]) > 0}
        # warnings = [x[1] for x in r]
        self.runtime_errors = errors
        return errors