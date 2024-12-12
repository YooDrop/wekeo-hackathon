import psycopg2
import numpy as np

from utils.config import load_db_config

def create_tables():
  commands = (
    """
    CREATE TABLE IF NOT EXISTS drops (
       id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
       lat decimal(10, 5) NOT NULL,
       lon decimal(10, 5) NOT NULL,
       depth decimal(15, 3) NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS position (
      id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
      drop_id uuid NOT NULL,
      lat decimal(10, 5) NOT NULL,
      lon decimal(10, 5) NOT NULL,
      depth decimal(15, 3) NOT NULL,
      created_at timestamptz NOT NULL,
      CONSTRAINT fk_drops
        FOREIGN KEY (drop_id)
        REFERENCES drops (id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS attributes (
      id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
      position_id uuid NOT NULL,
      attribute text,
      value decimal(10, 5),
      description text,
      CONSTRAINT fk_position
        FOREIGN KEY (position_id)
        REFERENCES position (id)
    );
    """
  )

  try:
    config = load_db_config()

    with psycopg2.connect(**config) as conn:
      with conn.cursor() as cur:
        for command in commands:
          cur.execute(command)
        
        conn.commit()

        # if we already have drops in the database skip initial 
        # drops creation
        drops = get_drops() 
        if(drops.size != 0):
          return

        initial_data_command = """
          INSERT INTO drops(lat, lon, depth) VALUES(%s,%s,%s) RETURNING id;
        """

        cur.execute(initial_data_command, (35.46, 162.96, 0.1))
        cur.execute(initial_data_command, (15.20, 162.45, 0.1))
        cur.execute(initial_data_command, (-36.55, -127.74, 0.1))
        cur.execute(initial_data_command, (33.32, -46.35, 0.1))

        conn.commit()

  except (psycopg2.DatabaseError, Exception) as error:
    print(error)

def get_drops():
  command = (
    """
    SELECT id, lat, lon, depth FROM drops;
    """
  )
  try:
    config = load_db_config()
    with psycopg2.connect(**config) as conn:
      with conn.cursor() as cur:
        # execute query command to retrieve all the drops
        cur.execute(command)
        # convert query result into numpy structured array to
        # make it easy to access attributes by their name
        result = np.array(
          cur.fetchall(), 
          dtype = np.dtype(
            [
              ('id', (np.str_, 36)), 
              ('lat', np.float32), 
              ('lon', np.float32), 
              ('depth', np.float32)
            ]
          )
        )
        return result;
  except (psycopg2.DatabaseError, Exception) as error:
    print(error)

def update_drop_position(id, lat, lon, depth, updated_at):
  command = (
    """
    UPDATE drops SET lat=%s, lon=%s, depth=%s WHERE id=%s;
    INSERT INTO position(drop_id, lat, lon, depth, created_at) VALUES (%s, %s, %s, %s, %s)
    RETURNING id
    """
  )
  try:
    config = load_db_config()
    with psycopg2.connect(**config) as conn:
      with conn.cursor() as cur:
        cur.execute(command, (lat, lon, depth, id, id, lat, lon, depth, updated_at));
        return cur.fetchone()[0];
  except (psycopg2.DatabaseError, Exception) as error:
    print(error)


def add_position_attribute(position_id, attribute, value, description):
  command = (
    """
    INSERT INTO attributes(position_id, attribute, value, description) VALUES (%s, %s, %s, %s)
    RETURNING id; 
    """
  )
  try:
    config = load_db_config()
    with psycopg2.connect(**config) as conn:
      with conn.cursor() as cur:
        cur.execute(command, (position_id, attribute, value, description));
        result = cur.fetchone()[0];
        return result;
  except (psycopg2.DatabaseError, Exception) as error:
    print(error)