import calendar
import copy
import json
import logging
import math
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from enum import IntEnum

from tqdm import tqdm

logging.basicConfig()
logger = logging.getLogger("EPJSON")
logger.setLevel(logging.DEBUG)


sched_type_limits = dict(
    key="SCHEDULETYPELIMITS",
    Name="Fraction",
    Lower_Limit_Value=0.0,
    Upper_Limit_Value=1.0,
    Numeric_Type="Continuous",
    Unit_Type="Dimensionless",
)

class ThermalMassConstructions(IntEnum):
    Concrete = 0
    Brick = 1
    WoodFrame = 2
    SteelFrame = 3


class ThermalMassCapacities(IntEnum):
    Concrete = 450000
    Brick = 100000
    WoodFrame = 50000
    SteelFrame = 20000


class HRV(IntEnum):
    NoHRV = 0
    Sensible = 1
    Enthalpy = 2


class Econ(IntEnum):
    NoEconomizer = 0
    DifferentialEnthalpy = 1
    # DifferentialDryBulb = 2


class MechVentMode(IntEnum):
    Off = 0
    AllOn = 1
    OccupancySchedule = 2


class EpJsonIDF:
    """
    A class for editing IDF files as EpJSONs (no archetypal dependence)
    """

    def __init__(
        self, idf_path, output_directory=None, eplus_loc=Path("C:\EnergyPlusV22-2-0")
    ):
        # get idf JSON
        self.eplus_location = Path(eplus_loc)
        self.idf_path = Path(idf_path)
        if output_directory:
            self.output_directory = output_directory
        else:
            self.output_directory = self.idf_path.parent
        # try:
        # check if epjson exists
        if not os.path.isfile(self.epjson_path):
            # if not, convert
            new_file = self.convert(
                str(self.idf_path),
                self.eplus_location,
                str(self.output_directory),
                file_type="epjson",
            )
            assert new_file == self.epjson_path
        # except Exception as e:
        #     logger.error("Error with conversion.")
        #     logger.error(e)
            # self.epjson_path = str(self.idf_path).split(".")[0] + ".epjson"
        with open(self.epjson_path, "r") as f:
            epjson = json.load(f)
            self.epjson = copy.deepcopy(epjson)

    @property
    def epjson_path(self):
        if hasattr(self, "_epjson_path") is False:
            base = self.idf_path.parent
            self._epjson_path = base / self.idf_path.name.lower().replace(".idf", ".epjson")
        return self._epjson_path
    
    @epjson_path.setter
    def epjson_path(self, epjson_path):
        self._epjson_path = epjson_path
        return self._epjson_path

    @classmethod
    def convert(cls, path, eplus_location, output_directory, file_type="epjson"):
        logger.info(f"Converting {path} to {file_type}")
        # Define the command and its arguments
        cmd = eplus_location / f"energyplus{'.exe' if os.name == 'nt' else ''}"
        logger.debug(cmd)
        args = ["--convert-only", "--output-directory", output_directory, path]
        logger.debug(args)

        # TODO change location of idf

        # Run the command
        with subprocess.Popen(
            [cmd] + args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        ) as proc:
            for line in proc.stdout:
                logger.info(line.strip())
            exit_code = proc.wait()

        # Check if the command was successful
        if exit_code == 0:
            logger.info("Command executed successfully.")
        else:
            logger.error(f"Command failed with exit code {exit_code}.")
            raise RuntimeError(f"Failed to convert EpJSON to IDF.")

        return str(path).split(".")[0] + f".{file_type}"

    def save(self):
        with open(self.epjson_path, "w") as f:
            json.dump(self.epjson, f, indent=4)

    def save_as(self, path):
        with open(path, "w") as f:
            json.dump(self.epjson, f, indent=4)

    def zone_update(self, key, zone_dict):
        for zone in self.epjson[key].values():
            zone.update(zone_dict)
    
    def save_idf(self, name=None, suffix=None, output_path=None):
        if output_path is None:
            output_path = self.output_directory
        if name:
            path = self.output_directory / name + ".epjson"
            self.save_as(path)
        elif suffix:
            path = str(self.epjson_path)[:-7] + suffix + ".epjson"
            self.save_as(path)
        else:
            path = self.epjson_path
            self.save()
        logger.info(f"Building idf for {path}")

        idf_path = self.convert(path, self.eplus_location, output_path, file_type="idf")
        return idf_path

    def replace_geometry(self, geometry_bunch):
        pass
    
    def fetch_features(self, features_map):
        '''
        Returns a pandas series of features for a given IDF, using a dictionary to map column names to idf entries.
        '''
        def _iter_paths(paths, zone=None, val_in=None):
            val_out = self.epjson
            for path in paths:
                n = path.replace("%ZONE%", zone)
                if val_in:
                    n = n.replace("%VAL%", str(val_in))
                val_out = val_out[n]
            return val_out
        
        features = {}
        # get zones
        zones = self.epjson["Zone"].keys()
        for zone in zones:
            zone_dict = {}
            for key, paths in features_map.items():
                if type(paths) == list:
                    # logger.info(f"Getting simple paths info for {key}")
                    try:
                        zone_dict[key] = _iter_paths(paths, zone)
                    except:
                        logger.warning(f"Cannot find {key} for zone {zone}")
                        zone_dict[key] = None

                elif type(paths) == dict:
                    if "path0" in paths.keys():
                        # logger.info(f"Getting nested paths info for {key}")
                        val = None
                        try:
                            for i, subpaths in enumerate(paths.values()):
                                if i < len(paths):
                                    val = _iter_paths(subpaths, zone, val)
                                else:
                                    val = _iter_paths(subpaths, zone)
                            zone_dict[key] = val
                        except:
                            logger.warning(f"Cannot find {key} for zone {zone}")
                            zone_dict[key] = None
                    elif "surface_type" in paths.keys():
                        construction_name = None
                        logger.info(f"Getting surface info for {key}")
                        if paths["surface_type"] == "Window":
                            windows = self.epjson["FenestrationSurface:Detailed"]
                            for window in windows.values():
                                if zone in window["building_surface_name"]:
                                    construction_name = window["construction_name"]
                                    break
                        else:
                            surfaces = self.epjson["BuildingSurface:Detailed"]
                            for surface in surfaces.values():
                                if (surface["surface_type"] == paths["surface_type"] and 
                                    surface["outside_boundary_condition"] == paths["outside_boundary_condition"] and
                                    surface["zone_name"] == zone
                                ):
                                    construction_name = surface["construction_name"]
                                    break
                        if construction_name:
                            if "conductivity" in paths.keys():
                                # Calculate U-value
                                val = self.calculate_u(self.epjson["Construction"][construction_name])
                            elif "specific_heat" in paths.keys():
                                # Calculate mass
                                val = self.calculate_mass(self.epjson["Construction"][construction_name])
                            zone_dict[key] = val
                        else:
                            logger.warning(f"Cannot find {key} for zone {zone}")
            features[zone] = zone_dict
        return features

    def calculate_u(self, construction):
        u_val = 0
        for layer_name in construction.values():
            material_def = None
            try:
                material_def = self.epjson["Material"][layer_name]
            except:
                try:
                    material_def = self.epjson["WindowMaterial:Glazing"][layer_name]
                except:
                    logger.debug("Gas layer, ignoring")
            if material_def:
                k = material_def["conductivity"]
                thick = material_def["thickness"]
                u_val += thick / k
        return 1/ u_val
    
    def calculate_mass(self, construction):
        tm = 0
        for layer_name in construction.values():
            material_def = self.epjson["Material"][layer_name]
            c = material_def["specific_heat"]
            thick = material_def["thickness"]
            rho = material_def["density"]
            tm_val = c * rho * thick
            tm += tm_val
        return tm * 10e-6

    def ach_to_infilration(self):
        pass

    def infiltration_to_ach(self):
        pass
    
    # @classmethod
    # def day_to_epbunch(cls, dsched, idx=0, sched_lim=sched_type_limits):
    #     return {
    #         dsched.Name: dict(
    #             **{"hour_{}".format(i + 1): dsched.all_values[i] for i in range(24)},
    #             schedule_type_limits_name=sched_lim["Name"],
    #         )
    #     }

    # @classmethod
    # def week_to_epbunch(cls, wsched, idx=0, sched_lim=sched_type_limits):
    #     return {
    #         wsched.Name: dict(
    #             **{
    #                 f"{calendar.day_name[i].lower()}_schedule_day_name": day.Name
    #                 for i, day in enumerate(wsched.Days)
    #             },
    #             holiday_schedule_day_name=wsched.Days[6].Name,
    #             summerdesignday_schedule_day_name=wsched.Days[0].Name,
    #             winterdesignday_schedule_day_name=wsched.Days[0].Name,
    #             customday1_schedule_day_name=wsched.Days[1].Name,
    #             customday2_schedule_day_name=wsched.Days[6].Name,
    #         )
    #     }

    # @classmethod
    # def year_to_epbunch(cls, sched, sched_lim=sched_type_limits):
    #     dict_list = []
    #     for i, part in enumerate(sched.Parts):
    #         dict_list.append(
    #             dict(
    #                 **{
    #                     "schedule_week_name": part.Schedule.Name,
    #                     "start_month": part.FromMonth,
    #                     "start_day".format(i + 1): part.FromDay,
    #                     "end_month".format(i + 1): part.ToMonth,
    #                     "end_day".format(i + 1): part.ToDay,
    #                 }
    #             )
    #         )
    #     return dict(
    #         schedule_type_limits_name=sched_lim["Name"],
    #         schedule_weeks=dict_list,
    #     )

    # @classmethod
    # def schedule_to_epbunch(cls, name, values, sched_lims_bunch=sched_type_limits):
    #     assert len(values) == 8760, "Schedule length does not equal 8760 hours!"
    #     arch_schedule = Schedule(Name=name, Values=values)
    #     y, w, d = arch_schedule.to_year_week_day()
    #     year_bunch = year_to_epbunch(y, sched_lims_bunch)
    #     week_bunches = []
    #     day_bunches = []
    #     for week in w:
    #         week_bunches.append(week_to_epbunch(week, sched_lims_bunch))
    #     for day in d:
    #         day_bunches.append(day_to_epbunch(day, sched_lims_bunch))
    #     return year_bunch, week_bunches, day_bunches


def validation_ventilation(epjson, mech_vent_sched_mode, new_filename):
    if mech_vent_sched_mode == MechVentMode.OccupancySchedule.value:
        pass
    elif mech_vent_sched_mode == MechVentMode.AllOn.value:
        epjson.zone_update(
            "DesignSpecification:OutdoorAir", {"outdoor_air_schedule_name": "AllOn"}
        )
        epjson.zone_update(
            "ZoneHVAC:IdealLoadsAirSystem",
            {"demand_controlled_ventilation_type": "None"},
        )
    else:
        epjson.zone_update(
            "DesignSpecification:OutdoorAir", {"outdoor_air_schedule_name": "Off"}
        )
        epjson.zone_update(
            "ZoneHVAC:IdealLoadsAirSystem",
            {"demand_controlled_ventilation_type": "None"},
        )
    epjson.save_idf(output_path=epjson.output_directory.parent / new_filename)
    # epjson.save_idf(suffix="_new") # original_name_new.idf instead of override


def process_idfs_for_ventilation(dat):
    idf_path = dat[0]
    mech_vent_sched_mode = dat[1]
    fname = dat[2]
    epjson = EpJsonIDF(idf_path)
    validation_ventilation(epjson, mech_vent_sched_mode, fname)
    os.remove(epjson.epjson_path)
    return idf_path


def fix_v8_outputs(idf_path):
    replace_summary = "SourceEnergyEndUseComponentsSummary"
    try:
        idf = EpJsonIDF(idf_path)
    except:
        pass
    idf.epjson["Output:SQLite"] = {
        "Output:SQLite 1": {
            "option_type": "Simple"
        }}
    for n, report_tables in idf.epjson["Output:Table:SummaryReports"].items():
        for reports in report_tables.values():
            for report in reports:
                if report["report_name"] == "Output:Table:SummaryReports":
                    report["report_name"] = replace_summary
                    idf.epjson["Output:Table:SummaryReports"][n]["reports"] = reports
    # Change outputs to hourly
    for n, vals in idf.epjson["Output:Variable"].items():
        vals.update({"reporting_frequency": "Hourly"})
        # idf.epjson["Output:Table:SummaryReports"][n]["reports"] = reports

    idf.save_idf()
    # os.remove(epjson.epjson_path)
    return idf_path

def fix_v8_outputs_parallel(idf_paths):
    with ThreadPoolExecutor(max_workers=8) as executor:
        idfs = list(tqdm(executor.map(fix_v8_outputs, idf_paths), total=len(idf_paths)))
    logger.info("Replaced all idfs.")

def set_validation_ventilation_schedules(hdf, fname):
    local_dir = Path(hdf).parent
    features = pd.read_hdf(hdf, key="buildings")
    names = features.index.to_list()
    vent_modes = features["VentilationMode"].to_list()
    idf_name = lambda x: "%09d" % (int(x),) + ".idf"
    idf_paths = [local_dir / "idf" / idf_name(x) for x in names]
    run_dict = [[x, y, fname] for x, y in zip(idf_paths, vent_modes)]

    with ThreadPoolExecutor(max_workers=8) as executor:
        dfs = list(tqdm(executor.map(process_idfs_for_ventilation, run_dict), total=len(run_dict)))
    logger.info("Downloading and opening files complete.")

    # delete extra files
    os.remove(local_dir / "idf" / "eplusout.end")
    os.remove(local_dir / "idf" / "eplusout.err")
    os.remove(local_dir / fname / "eplusout.end")
    os.remove(local_dir / fname / "eplusout.err")


# make a click function which accepts a number of simulations to run, a bucket name, and an experiment name
# @click.command()
# @click.option(
#     "--path", default=None, help=".hdf features path", prompt="Path to hdf file"
# )
# @click.option(
#     "--fname",
#     default="idf_new",
#     help="Name for file to store altered IDFs. If not set will override.",
#     # prompt="Name for file to store altered IDFs. If not set will override.",
# )
# @click.option(
#     "--log_level",
#     default="ERROR",
#     help="Logging level",
#     prompt="Logging level",
# )
def main(path, fname, log_level):
    logger.setLevel(log_level)
    paths = os.listdir(path)
    paths = [Path(path) / x for x in paths if ".idf" in x]
    print(paths)
    fix_v8_outputs_parallel(paths)
    # set_validation_ventilation_schedules(path, fname)


if __name__ == "__main__":
    import pandas as pd

    # mech_vent_sched_mode = 2
    # epjson = EpJsonIDF(
    #     "D:/DATA/validation_v2/idf/000000000.idf",
    # )
    # validation_ventilation(epjson, mech_vent_sched_mode)

    main()
    # hdf_path = "./ml-for-bem/data/temp/validation/v3/features.hdf"
