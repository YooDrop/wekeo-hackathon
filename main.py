import copernicusmarine
import psycopg2

from datetime import date, datetime
from geopy import Point, distance
from os import environ

from utils.db import create_tables, get_drops, update_drop_position, add_position_attribute
from utils.move import get_direction, get_speed
from utils.config import load_db_config

username = environ['CM_USERNAME']
password = environ['CM_PASSWORD']
iterations = int(environ['CM_ITERATIONS'])


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
    service="arco-time-series"
  )
  return dataset

def calculate_drops(conn):

  drops = get_drops(conn) 

  for drop in drops:
    # retrieve data sets
    uv = get_dataset("cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m", drop["lon"], drop["lat"], drop["depth"], ["uo", "vo"])
    w = get_dataset("cmems_mod_glo_phy-wcur_anfc_0.083deg_P1D-m", drop["lon"], drop["lat"], drop["depth"], ["wo"])
    t = get_dataset("cmems_mod_glo_phy_anfc_0.083deg_P1D-m", drop["lon"], drop["lat"], drop["depth"], ["tob"])  # Temperature [degree C]
    p = get_dataset("cmems_mod_glo_phy_anfc_0.083deg_P1D-m", drop["lon"], drop["lat"], drop["depth"], ["pbo"])  # Pressure [dbar]
    s = get_dataset("cmems_mod_glo_phy_anfc_0.083deg_P1D-m", drop["lon"], drop["lat"], drop["depth"], ["sob"])  # Salinity [parts per thousand or 10^-3]
    # h = get_dataset("cmems_obs-wave_glo_phy-swh_nrt_multi-l4-2deg_P1D-i", drop["lon"], drop["lat"], drop["depth"], ["VAVH_INST"]) # [m]
    c = get_dataset("cmems_mod_glo_bgc-pft_anfc_0.25deg_P1D-m", drop["lon"], drop["lat"], drop["depth"], ["chl"]) # [mg/m3]

    # uo horizontal speed component
    # vo horizontal speed component
    # wo vertical speed component
    uo = float(uv.uo.sel(latitude = drop["lat"], longitude = drop["lon"], depth = drop["depth"], time = date.today(), method = "nearest"))
    vo = float(uv.vo.sel(latitude = drop["lat"], longitude = drop["lon"], depth = drop["depth"], time = date.today(), method = "nearest"))
    wo = float(w.wo.sel(latitude = drop["lat"], longitude = drop["lon"], depth = drop["depth"], time = date.today(), method = "nearest"))

    temperature = float(t.tob.sel(latitude=drop["lat"], longitude=drop["lon"], time=date.today(), method="nearest"))
    pressure = float(p.pbo.sel(latitude=drop["lat"], longitude=drop["lon"], time=date.today(), method="nearest"))
    salinity = float(s.sob.sel(latitude=drop["lat"], longitude=drop["lon"], time=date.today(), method="nearest"))
    # height = float(h.VAVH_INST.sel(latitude=drop["lat"], longitude=drop["lon"], time=date.today(), method="nearest"))
    chlorophyll = float(c.chl.sel(latitude=drop["lat"], longitude=drop["lon"], depth = drop["depth"], time=date.today(), method="nearest"))


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
    position_id = update_drop_position(conn, drop["id"], next_point.latitude, next_point.longitude, next_depth, datetime.now())
    # position id might be useful to add additional attributes related to current drop
    # position
    print('New drop position id {0}'.format(position_id));

    add_position_attribute(conn, position_id, 'temperature', temperature, 'Temperature at this position of the drop')
    add_position_attribute(conn, position_id, 'pressure', pressure, 'Pressure at this position of the drop')
    add_position_attribute(conn, position_id, 'salinity', salinity, 'Salinity at this position of the drop')
    # add_position_attribute(conn, position_id, 'height', height, 'Instant Significant Wave Height at this position of the drop')
    add_position_attribute(conn, position_id, 'chlorophyll', chlorophyll, 'Mass concentration of chlorophyll-a at this position of the drop')

def main():
  current_iteration = 0

  try:
    with psycopg2.connect(**load_db_config()) as conn:
      # create tables and initial data if it does not exist
      create_tables(conn)
      # login into copernicus data store system, skip if user already has been logged in
      copernicusmarine.login(username=username, password=password, skip_if_user_logged_in=True)
      # simulate movement defined times
      while current_iteration < iterations:
        current_iteration = current_iteration + 1;
        print('Current iteration {0}'.format(current_iteration))
        calculate_drops(conn)
      # close collection after simulation finishes      
      conn.close()
  except(psycopg2.DatabaseError, Exception) as error:
    print(error);

if __name__ == '__main__':
  main()
