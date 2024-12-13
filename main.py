import copernicusmarine

from datetime import date, datetime
from geopy import Point, distance
from os import environ

from utils.db import create_tables, get_drops, update_drop_position, add_position_attribute
from utils.move import get_direction, get_speed

username = environ['CM_USERNAME']
password = environ['CM_PASSWORD']
iterations = environ['CM_ITERATIONS']

copernicusmarine.login(username=username, password=password)

def get_dataset(id, lon, lat, depth, variables):
  dataset = copernicusmarine.open_dataset(
    dataset_id = id,
    minimum_longitude = lon,
    maximum_longitude = lon,
    minimum_latitude = lat,
    maximum_latitude = lat,
    minimum_depth = depth,
    maximum_depth = depth,
    start_datetime = date.today().strftime("%Y-%m-%d"),
    end_datetime = date.today().strftime("%Y-%m-%d"),
    variables = variables,
    dataset_part = "default",
    dataset_version = "202406",
    service="arco-time-series"
  )
  return dataset

def main():
  create_tables()

  drops = get_drops() 

  current_iteration = 0
  while current_iteration < int(iterations):
    current_iteration = current_iteration + 1
    print("Iteration {0}".format(current_iteration))
    for drop in drops:
      # retrieve data sets
      uv = get_dataset("cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m", drop["lon"], drop["lat"], drop["depth"], ["uo", "vo"])
      w = get_dataset("cmems_mod_glo_phy-wcur_anfc_0.083deg_P1D-m", drop["lon"], drop["lat"], drop["depth"], ["wo"])
      t = get_dataset("cmems_mod_glo_phy_anfc_0.083deg_P1D-m", drop["lon"], drop["lat"], drop["depth"], ["tob"])  # Temperature [degree C]
      p = get_dataset("cmems_mod_glo_phy_anfc_0.083deg_P1D-m", drop["lon"], drop["lat"], drop["depth"], ["pbo"])  # Pressure [dbar]
      s = get_dataset("cmems_mod_glo_phy_anfc_0.083deg_P1D-m", drop["lon"], drop["lat"], drop["depth"], ["sob"])  # Salinity [parts per thousand or 10^-3]

      # uo horizontal speed component
      # vo horizontal speed component
      # wo vertical speed component
      uo = float(uv.uo.sel(latitude = drop["lat"], longitude = drop["lon"], depth = drop["depth"], time = date.today(), method = "nearest"))
      vo = float(uv.vo.sel(latitude = drop["lat"], longitude = drop["lon"], depth = drop["depth"], time = date.today(), method = "nearest"))
      wo = float(w.wo.sel(latitude = drop["lat"], longitude = drop["lon"], depth = drop["depth"], time = date.today(), method = "nearest"))

      temperature = float(t.tob.sel(latitude=drop["lat"], longitude=drop["lon"], time=date.today(), method="nearest"))
      pressure = float(p.pbo.sel(latitude=drop["lat"], longitude=drop["lon"], time=date.today(), method="nearest"))
      salinity = float(s.sob.sel(latitude=drop["lat"], longitude=drop["lon"], time=date.today(), method="nearest"))

      # calculating horizontal distance and direction in
      # order to calculate next geo point of the drop
      horizontal_speed = get_speed(uo, vo)
      horizontal_direction = get_direction(uo, vo)
      horizontal_distance = horizontal_speed * 3600

      # calculating next get point according to horizontal
      # distance and horizontal direction
      start_point = Point(drop["lat"], drop["lon"])
      next_point = distance.geodesic(meters=horizontal_distance).destination(start_point, horizontal_direction)

      # next depth
      next_depth = drop["depth"] + wo * 3600
      if next_depth < 0:
        next_depth = 0;

      # update data base, save current drop position and
      # add new movement position
      position_id =update_drop_position(drop["id"], next_point.latitude, next_point.longitude, next_depth, datetime.now())

      add_position_attribute(position_id, 'temperature', temperature, 'Temperature at this position of the drop')
      add_position_attribute(position_id, 'pressure', pressure, 'Pressure at this position of the drop')
      add_position_attribute(position_id, 'salinity', salinity, 'Salinity at this position of the drop')

if __name__ == '__main__':
  main()
