import copy
import json
import logging
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from enum import IntEnum
from tqdm import tqdm
import pandas as pd
import eppy.geometry.surface as epsurface

from epjson.sql import Sql

logging.basicConfig()
logger = logging.getLogger("EPJSON")
logger.setLevel(logging.INFO)


# sched_type_limits = dict(
#     key="SCHEDULETYPELIMITS",
#     Name="Fraction",
#     Lower_Limit_Value=0.0,
#     Upper_Limit_Value=1.0,
#     Numeric_Type="Continuous",
#     Unit_Type="Dimensionless",
# )

# class ThermalMassConstructions(IntEnum):
#     Concrete = 0
#     Brick = 1
#     WoodFrame = 2
#     SteelFrame = 3


# class ThermalMassCapacities(IntEnum):
#     Concrete = 450000
#     Brick = 100000
#     WoodFrame = 50000
#     SteelFrame = 20000


# class HRV(IntEnum):
#     NoHRV = 0
#     Sensible = 1
#     Enthalpy = 2


# class Econ(IntEnum):
#     NoEconomizer = 0
#     DifferentialEnthalpy = 1
#     # DifferentialDryBulb = 2


class MechVentMode(IntEnum):
    Off = 0
    AllOn = 1
    OccupancySchedule = 2

SIMPLE_GLAZING_TEMPLATE =  {
        "SimpleGlazing": {
            "solar_heat_gain_coefficient": 0.65,
            "u_factor": 1,
            "visible_transmittance": 0.8
        }}

class EpJsonIDF:
    """
    A class for editing IDF files as EpJSONs (no archetypal dependence)
    """

    def __init__(
        self, idf_path, epjson_path, output_directory=None, eplus_loc=Path("C:\EnergyPlusV22-2-0")
    ):
        # get idf JSON
        self.eplus_location = Path(eplus_loc)
        self.idf_path = Path(idf_path)
        self.epjson_path = Path(epjson_path)
        self.output_directory = Path(output_directory)
        
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
    def upgrade(cls, path, eplus_location, ver_from="8-4-0", ver_to="22-2-0"):
        version = Path(eplus_location).name
        logger.info(f"Upgrading {path} to {version}")
        # TODO
        # Define the command and its arguments
        # cmd = eplus_location / "PreProcess" / "IDFVersionUpdater" / f"Transition-V{ver_from}-to-V{ver_to}.exe"
        # logger.debug(cmd)
        # args = ["--convert-only", "--output-directory", output_directory, path]
        # logger.debug(args)

        # # TODO change location of idf

        # # Run the command
        # with subprocess.Popen(
        #     [cmd] + args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        # ) as proc:
        #     for line in proc.stdout:
        #         logger.info(line.strip())
        #     exit_code = proc.wait()

        # # Check if the command was successful
        # if exit_code == 0:
        #     logger.info("Command executed successfully.")
        # else:
        #     logger.error(f"Command failed with exit code {exit_code}.")
        #     raise RuntimeError(f"Failed to convert EpJSON to IDF.")

        # return str(path).split(".")[0] + f".{file_type}"

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
        return Path(output_directory) / Path(path).name.replace(".idf", "epJSON") # TODO not always that way

    @classmethod
    def parallel_convert(cls, files_and_dirs, eplus_location, file_type="epjson"):
        def _parallel_convert(in_tuple):
            path, out_dir = in_tuple
            loc = eplus_location
            tp = file_type
            cls.convert(path=path, eplus_location=loc, output_directory=out_dir, file_type=tp)
        with ThreadPoolExecutor(max_workers=8) as executor:
            idfs = list(tqdm(executor.map(_parallel_convert, files_and_dirs), total=len(files_and_dirs)))
        logger.info("Converted all idfs.")
        return idfs
    
    @classmethod
    def run(cls, idf_path, eplus_location, epw, verbose=False):
        # Define the command and its arguments
        out_dir = Path(idf_path).parent
        cmd = eplus_location / f"energyplus{'.exe' if os.name == 'nt' else ''}"
        args = ["-w", epw, "-d", out_dir, idf_path]

        # Run the command
        with subprocess.Popen(
            [cmd] + args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        ) as proc:
            for line in proc.stdout:
                if verbose:
                    logger.info(line.strip())
            exit_code = proc.wait()

        # Check if the command was successful
        if exit_code == 0:
            logger.debug("Command executed successfully.")
        else:
            logger.error(f"Command failed with exit code {exit_code} for {idf_path}.")
            # raise RuntimeError(f"Failed to simulate IDF.")

    @classmethod
    def error_report(cls, idf_path, results_name="eplusout.err"):
        SEVERE = "** Severe  **"
        FATAL = "**  Fatal  **"
        WARNING = "** Warning **"
        NEXTLINE = "**   ~~~   **"
        filepath = Path(idf_path).parent / results_name
        errors = []
        warnings = []
        with open(filepath, "r") as f:
            for line in f:
                l = line.strip()
                if l.startswith(FATAL):
                    errors.append(l)
                elif l.startswith(SEVERE) or l.startswith(FATAL):
                    errors.append(l)
                elif l.startswith(WARNING):
                    warnings.append(l)
        if len(errors) > 0:
            logger.warning(f"Errors with {idf_path}")
            logger.warning(errors)
        return (errors, warnings)
    
    @classmethod
    def process_results(cls, idf_path, selected_outputs, reporting_frequency="Hourly"):
        logger.debug(f"Fetching SQL results for {Path(idf_path).name}")
        results_path = Path(idf_path).parent / "eplusout.sql"
        sql = Sql(results_path)
        ep_df_hourly = pd.DataFrame(
            sql.timeseries_by_name(
                selected_outputs, reporting_frequency
            )
        )
        return ep_df_hourly

    # @classmethod
    # def read_vars_eso(self, path, eplus_location):
    #     # Define the command and its arguments
    #     eplus_postprocess = Path(eplus_location) / "PostProcess"
    #     cmd =  eplus_postprocess / "RunReadESO.bat"

    #     # Copy the eso into the eplus location
    #     shutil.copyfile(Path(path).parent / "eplusout.eso", eplus_postprocess / "eplusout.eso")

    #     # Run the command
    #     with subprocess.Popen(
    #         [cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    #     ) as proc:
    #         for line in proc.stdout:
    #             logger.info(line.strip())
    #         exit_code = proc.wait()

    #     # Check if the command was successful
    #     if exit_code == 0:
    #         shutil.copyfile(eplus_postprocess / "eplusout.csv", Path(path).parent / "eplusout.csv")
    #         logger.info("Command executed successfully.")
    #     else:
    #         logger.error(f"Command failed with exit code {exit_code}.")
    #         raise RuntimeError(f"Failed to convert EpJSON to IDF.")
    #     return Path(eplus_location).parent / "eplusout.csv"

    def save(self):
        with open(self.epjson_path, "w") as f:
            json.dump(self.epjson, f, indent=4)

    def save_as(self, path):
        path = Path(path)
        if not os.path.isdir(path.parent):
            os.makedirs(path.parent)
        with open(path, "w") as f:
            json.dump(self.epjson, f, indent=4)

    def zone_values(self, toplevel_key, key):
        vals = []
        for zone in self.epjson[toplevel_key].values():
            vals.append(zone[key])
        return vals

    def zone_update(self, key, zone_dict):
        for zone in self.epjson[key].values():
            zone.update(zone_dict)
    
    def zone_delete(self, toplevel_key, bottomlevel_key):
        for zone in self.epjson[toplevel_key].values():
            del zone[bottomlevel_key]
    
    def update_schedules(self, schedules_dict):
        #TODO check if already exists in other annual schedules
        for key, dat in schedules_dict.items():
            assert key in self.epjson.keys()
            self.epjson[key].update(dat)

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

    def remove(self, toplevel_key):
        if toplevel_key in self.epjson.keys():
            del self.epjson[toplevel_key]
        else:
            logger.warning(f"{toplevel_key} not found in EPJSON.")

    def add(self, epbunches):
        assert type(epbunches) == list
        for toplevel_key, bunch in epbunches:
            if toplevel_key in self.epjson.keys():
                logger.info(f"Updating definition of {toplevel_key}")
                self.epjson[toplevel_key].update(bunch)
            else:
                self.epjson[toplevel_key] = bunch
    
    def replace_geometry(self, geometry_bunch):
        pass

    def _surface_area(self, surface):
        poly = [tuple(x.values()) for x in surface["vertices"]]
        area = epsurface.area(poly)
        return area

    def calculate_zone_areas(self):
        zone_areas = {}
        zones = self.epjson["Zone"].keys()
        for zone in zones:
            area = 0
            surfaces = self.epjson["BuildingSurface:Detailed"]
            for surface in surfaces.values():
                if surface["surface_type"] == "Floor" and surface["zone_name"] == zone:
                    area += self._surface_area(surface)
            zone_areas[zone] = area
        self.zone_areas = zone_areas
        return zone_areas

    def _iter_paths(self, paths, zone=None, val_in=None):
            val_out = self.epjson
            for path in paths:
                n = path.replace("%ZONE%", zone)
                if val_in:
                    n = n.replace("%VAL%", str(val_in))
                val_out = val_out[n]
            return val_out
    
    def fetch_features(self, features_map):
        '''
        Returns a pandas series of features for a given IDF, using a dictionary to map column names to idf entries.
        '''
        
        features = {}
        # get zones
        zones = self.epjson["Zone"].keys()
        for zone in zones:
            zone_dict = {}
            for key, paths in features_map.items():
                if type(paths) == list:
                    # logger.info(f"Getting simple paths info for {key}")
                    try:
                        zone_dict[key] = self._iter_paths(paths, zone)
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
                                    val = self._iter_paths(subpaths, zone, val)
                                else:
                                    val = self._iter_paths(subpaths, zone)
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
                                vals = self.calculate_r(self.epjson["Construction"][construction_name])
                                val = sum(x for _, x, _ in vals)
                                if paths["surface_type"] == "Window":
                                    # Calculate U-value
                                    val = 1/val
                            elif "specific_heat" in paths.keys():
                                # Calculate mass
                                val = self.calculate_mass(self.epjson["Construction"][construction_name])
                                val = sum(x for _, x, _ in vals) # * 10e-6
                            zone_dict[key] = val
                        else:
                            logger.warning(f"Cannot find {key} for zone {zone}")
            features[zone] = zone_dict
        return features

    def replace_features(self, features, features_map):
        '''
        replaces values in an epJSON from a pandas series of features
        '''
        
        zones = list(self.epjson["Zone"].keys())
        for key in features.index:
            try:
                k = float(features[key])
            except:
                k = features[key]

            logger.info(f"Setting {key} to {k}")
            paths = features_map[key]
            try:
                if type(paths) == list:
                    self.zone_update(
                        paths[0], {paths[-1]: k}
                    )

                elif type(paths) == dict:
                    if "path0" in paths.keys():
                        logger.debug(f"Nested setting, {key}")
                        for zone in zones:
                            val = None
                            for i, subpaths in enumerate(paths.values()):
                                if i < len(paths)-1:
                                    val = self._iter_paths(subpaths, zone, val)
                                else:
                                    if "schedule" in subpaths[0].lower():
                                        self.epjson[subpaths[0]].update({val: {subpaths[-1]: k}})
                                    else:
                                        self.zone_update(
                                            subpaths[0], {subpaths[-1]: k}
                                            )
                                
                    elif "surface_type" in paths.keys():
                        logger.debug(f"Getting surface info for {key}")
                        if paths["surface_type"] == "Window": # TODO: what if window already has simple glazing
                            logger.debug("Setting simple window settings.")
                            windows = self.epjson["FenestrationSurface:Detailed"]
                            if "WindowMaterial:SimpleGlazingSystem" not in self.epjson.keys():
                                logger.warning("Replacing windows with simple window")
                                construction_name = windows[list(windows.keys())[0]]["construction_name"]
                                self.epjson["Construction"][construction_name] = {"outside_layer": "SimpleGlazing"}
                                self.epjson["WindowMaterial:SimpleGlazingSystem"] = SIMPLE_GLAZING_TEMPLATE
                            if "conductivity" in paths.keys():
                                self.epjson["WindowMaterial:SimpleGlazingSystem"]["SimpleGlazing"].update({"u_factor": k})
                            elif "solar_heat_gain_coefficient" in paths.keys():
                                self.epjson["WindowMaterial:SimpleGlazingSystem"]["SimpleGlazing"].update({"solar_heat_gain_coefficient": k})
                        else:
                            logger.debug("Setting opaque surface.")
                            surfaces = self.epjson["BuildingSurface:Detailed"]
                            for zone in zones:
                                for surface in surfaces.values():
                                    if (surface["surface_type"] == paths["surface_type"] and 
                                        surface["outside_boundary_condition"] == paths["outside_boundary_condition"] and
                                        surface["zone_name"] == zone
                                    ):
                                        construction_name = surface["construction_name"]
                                        break
                            if "conductivity" in paths.keys():
                                self.change_construction_r(self.epjson["Construction"][construction_name], k)
                            elif "specific_heat" in paths.keys():
                                #TODO skipping mass now
                                self.change_construction_mass(self.epjson["Construction"][construction_name], k * 10e6)
                    else:
                        for _, paths2 in paths.items():
                            self.zone_update(
                                paths2[0], {paths2[-1]: k}
                            )
            except Exception as e:
                logger.warning(f"Missing {key}...")
                if key == "ScheduledVentilationRate":
                    self.add_scheduled_ventilation(k)
                else:
                    # logger.error(e)
                    raise(e)
            # self.save()


    def _calculate_layer_r(self, material_def):
        k = material_def["conductivity"]
        thick = material_def["thickness"]
        return thick / k
    
    def calculate_u(self, construction):
        u_vals = []
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
                u_vals.append((layer_name, 1/self._calculate_layer_r(material_def), material_def))
        return u_vals
    
    def calculate_r(self, construction):
        r_vals = []
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
                r_vals.append((layer_name, self._calculate_layer_r(material_def), material_def))
        return r_vals

    def _calculate_layer_mass(self, material_def):
        c = material_def["specific_heat"]
        thick = material_def["thickness"]
        rho = material_def["density"]
        return c * rho * thick

    def calculate_mass(self, construction):
        tms = []
        for layer_name in construction.values():
            material_def = self.epjson["Material"][layer_name]
            tms.append((layer_name, self._calculate_layer_mass(material_def), material_def))
        return tms
    
    def change_construction_r(self, construction, new_r):
        """
        Change a Construction's insulation layer to reach a target u
        """
        r_vals = self.calculate_r(construction)
        current_r_val = sum(r_val for _, r_val, _ in r_vals)
        logger.debug(f"Old R-val: {current_r_val}")
        sorted_layers = sorted(r_vals, key=lambda x: -x[1])
        insulator = sorted_layers[0]
        logger.debug(f"Found insulator {insulator[0]}")
        r_insulator_current = insulator[1]
        insulator_def = insulator[2]
        r_val_without_insulator = current_r_val - r_insulator_current
        needed_r = new_r - r_val_without_insulator
        # assert needed_r > 0
        if needed_r > 0:
            new_thickness = needed_r * insulator_def["conductivity"]
            if new_thickness < 0:
                raise ValueError("Desired R-value and TM combo is not possible.")
            if new_thickness < 0.003:
                logger.warning(
                    "Thickness of insulation is less than 0.003. This will create a warning in EnergyPlus."
                )
        else:
            #TODO: delete layer or repeat with next layer
            new_thickness = 0.003
        logger.debug(f"New thickness {new_thickness}")
        insulator_def["thickness"] = round(new_thickness, 3)
        new_r_vals = self.calculate_r(construction)
        logger.debug(
            f"New R-val = {sum(r for _, r, _ in new_r_vals)} compared to desired {new_r}"
        )

    def change_construction_mass(self, construction, new_tm):
        """
        specific heat (c) of the material layer from template is in units of J/(kg-K)
        density is in kg/m3
        thermal mass is in J/Km2 TM = c * density * thickness
        """
        r = self.calculate_r(construction)
        r = sum(x for _, x, _ in r)
        tm_vals = self.calculate_mass(construction)
        current_tm = sum(x for _, x, _ in tm_vals)
        logger.debug(f"Old thermal mass: {current_tm}, and RVal: {r}")
        sorted_layers = sorted(tm_vals, key=lambda x: -x[1])
        mass = sorted_layers[0]
        logger.debug(f"Found massive layer {mass[0]}")
        tm_mass_current = mass[1]
        mass_def = mass[2]
        tm_without_insulator = current_tm - tm_mass_current
        needed_tm = new_tm - tm_without_insulator
        assert needed_tm > 0
        new_thickness = needed_tm / mass_def["specific_heat"] / mass_def["density"]
        if new_thickness < 0:
            raise ValueError("Desired R-value and TM combo is not possible.")
        if new_thickness < 0.003:
            logger.warning(
                "Thickness of insulation is less than 0.003. This will create a warning in EnergyPlus."
            )
        logger.debug(f"New thickness {new_thickness}")
        mass_def["thickness"] = round(new_thickness, 3)
        new_tm_vals = self.calculate_mass(construction)
        logger.debug(
            f"New thermal mass = {sum(t for _, t, _ in new_tm_vals)} compared to desired {new_tm}"
        )
        # Check if mass/rvalue combo is possible
        logger.debug("Recalculating r-values...")
        self.change_construction_r(construction, r)
        
    def ach_to_infilration(self):
        pass

    def infiltration_to_ach(self):
        pass

    def add_scheduled_ventilation(self, air_changes_per_hour):
        zones = self.epjson["Zone"].keys()
        vent_dict = {}
        for zone in zones:
            vent_dict[f"{zone} ScheduledVent"] = {
                "air_changes_per_hour": air_changes_per_hour,
                "constant_term_coefficient": 1,
                "delta_temperature": 1,
                "design_flow_rate_calculation_method": "AirChanges/Hour",
                "fan_pressure_rise": 0,
                "fan_total_efficiency": 1,
                "maximum_wind_speed": 40,
                "minimum_indoor_temperature": 18,
                "schedule_name": "AllOn",
                "temperature_term_coefficient": 0,
                "velocity_squared_term_coefficient": 0,
                "velocity_term_coefficient": 0,
                "ventilation_type": "Natural",
                "zone_or_zonelist_or_space_or_spacelist_name": zone
            }
        self.epjson["ZoneVentilation:DesignFlowRate"] = vent_dict

    @classmethod
    def from_idf(cls, idf_path, eplus_loc=Path("C:\EnergyPlusV22-2-0"), **kwargs):
        idf_path = Path(idf_path)
        base = idf_path.parent
        _epjson_path = base / idf_path.name.replace(".idf", ".epJSON")


        if "output_directory" in kwargs:
            output_directory = kwargs.get("output_directory")
        else:
            output_directory = _epjson_path.parent

        # check if epjson exists
        if not os.path.isfile(_epjson_path):
            # if not, convert
            new_file = cls.convert(
                str(idf_path),
                eplus_loc,
                str(output_directory),
                file_type="epjson",
            )
                    # except Exception as e:
        #     logger.error("Error with conversion.")
        #     logger.error(e)
            # self.epjson_path = str(self.idf_path).split(".")[0] + ".epjson"
        return cls(idf_path=idf_path, epjson_path=_epjson_path, output_directory=output_directory, eplus_loc=eplus_loc)
        

    @classmethod
    def from_epjson(cls, epjson_path, eplus_loc=Path("C:\EnergyPlusV22-2-0"), **kwargs):
        epjson_path = Path(epjson_path)
        base = epjson_path.parent
        idf_path = base / epjson_path.name.replace(".epJSON", ".idf")
    
        if "output_directory" in kwargs:
            output_directory = kwargs.get("output_directory")
        else:
            output_directory = epjson_path.parent
            
        if not os.path.isfile(idf_path):
            # if not, convert
            new_file = cls.convert(
                str(epjson_path),
                eplus_loc,
                str(output_directory),
                file_type="idf",
            )
            # assert new_file == idf_path

        return cls(idf_path=idf_path, epjson_path=epjson_path, output_directory=output_directory, eplus_loc=eplus_loc)
    
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
