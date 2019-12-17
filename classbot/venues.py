import numpy as np
import pandas as pd

from parameters.param import classbot_directory

venues_filename = classbot_directory + 'venues.npy'


def get_venues():
    return np.load(venues_filename, allow_pickle='TRUE').item()


def add_venue(venue_name, venue_id):
    venues = get_venues()
    venues[venue_name] = str(venue_id)
    np.save(venues_filename, venues)


def remove_venue(venue_name):
    venues = get_venues()
    venues.pop(venue_name, None)
    np.save(venues_filename, venues)


def get_venues_for_front():
    return pd.Series(get_venues()).rename('id').rename_axis('name').reset_index().to_json(orient="records")
