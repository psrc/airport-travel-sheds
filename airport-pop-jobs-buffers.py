# This script determines if a parcel is within the Airport Travel Time Buffer
# Created by Puget Sound Regional Council Staff
# March 2021

import h5py
import pandas as pd 
import os
from shapely.geometry import Point
import geopandas as gp
import sqlalchemy
from shapely import wkt

model_year = 2050
shed_year = 2050
airports = ['paine']
shed_name = 'paine'

# Basic Inputs
model_outputs_folder = 'C:\\model-outputs'
project_directory = 'C:\\projects\\airport\\wsdot'
buffer_folder = os.path.join(project_directory,'buffers')
spn = 'epsg:2285'

connection_string = 'mssql+pyodbc://AWS-PROD-SQL\Sockeye/ElmerGeo?driver=SQL Server?Trusted_Connection=yes'

# Functions
def create_df_from_h5(h5_file, h5_table, h5_variables):

    h5_data = {}
    
    for var in h5_variables:
        h5_data[var] = h5_file[h5_table][var][:]
    
    return pd.DataFrame(h5_data)

def create_point_from_table(current_df,x_coord,y_coord,coord_sys):
    current_df['geometry'] = current_df.apply(lambda x: Point((float(x[x_coord]), float(x[y_coord]))), axis=1)
    geo_layer = gp.GeoDataFrame(current_df, geometry='geometry')
    geo_layer.crs = {'init' :coord_sys}
    
    return geo_layer

def read_from_sde(connection_string, feature_class_name, crs = {'init' :'epsg:2285'}):
    engine = sqlalchemy.create_engine(connection_string)
    con=engine.connect()
    feature_class_name = feature_class_name + '_evw'
    df=pd.read_sql('select *, Shape.STAsText() as geometry from %s' % (feature_class_name), con=con)
    con.close()
    df['geometry'] = df['geometry'].apply(wkt.loads)
    gdf=gp.GeoDataFrame(df, geometry='geometry')
    gdf.crs = crs
    cols = [col for col in gdf.columns if col not in ['Shape', 'GDB_GEOMATTR_DATA', 'SDE_STATE_ID']]
    return gdf[cols]

print('Loading Full TAZ shapefile')
zones = read_from_sde(connection_string, 'taz2010')
cols = ['taz','geometry']
zones = zones[cols]

print('Loading travel sheds for airports')
current_count = 0
 
for current_airport in airports:
    
    sheds = pd.read_excel(os.path.join(buffer_folder,'travel_sheds_60.xlsx'),sheet_name=current_airport)
    cols = ['zone',shed_year]
    sheds = sheds[cols]
    colnames = ['taz',current_airport]
    sheds.columns = colnames
    
    if current_count == 0:
        airport_shed = sheds
        
    else:
        airport_shed = pd.merge(airport_shed, sheds, on='taz',suffixes=('_x','_y'),how='left')
        
    current_count = current_count + 1

airport_shed['combined'] = airport_shed.loc[:,airports].sum(axis=1) 

print('Selecting zones that are in the selected airport travel sheds')
buffer = pd.merge(zones, airport_shed, on='taz')
buffer = buffer[buffer['combined'] >0]

print('Write buffer file to disk for mapping of travel sheds')
buffer.to_file(os.path.join(buffer_folder,"airport_buffers_" + shed_name + '_' + str(model_year) + ".shp"))

print ('Create a parcel file with X,Y Coordinates to join final data with')
parcel_cols = ['parcelid','xcoord_p','ycoord_p']
parcels_xy = pd.read_csv(os.path.join(model_outputs_folder,str(model_year),'parcels_urbansim.txt'), sep = ' ')
parcels_xy.columns = parcels_xy.columns.str.lower()
parcels_xy = parcels_xy.loc[:,parcel_cols]

print ('Creating parcel file with people and jobs for ' + str(model_year)) 
person_variables=['hhno']
hh_variables=['hhno','hhparcel']
parcel_cols = ['parcelid','emptot_p']

print('Create a parcel dataframe with jobs')
wrk_prcls = pd.read_csv(os.path.join(model_outputs_folder,str(model_year),'parcels_urbansim.txt'), sep = ' ')
wrk_prcls.columns = wrk_prcls.columns.str.lower()
wrk_prcls = wrk_prcls.loc[:,parcel_cols]

print('Create HH and Person dataframes from the h5 File')
hh_people = h5py.File(os.path.join(model_outputs_folder, str(model_year), 'hh_and_persons.h5'),'r+') 
hh_df = create_df_from_h5(hh_people, 'Household', hh_variables)
person_df = create_df_from_h5(hh_people, 'Person', person_variables)
    
print('Create a HH file by household number with total population')
person_df['population'] = 1
df_hh = person_df.groupby('hhno').sum().reset_index()

print('Merge the HH File created from the persons with the original HH file to get parcel id')
df_hh = pd.merge(df_hh,hh_df,on='hhno',suffixes=('_x','_y'),how='left')
df_hh = df_hh.drop(['hhno'],axis=1)

print('Group the HH Files by Parcel ID so it can be merged with master parcel file')
df_parcel_hh = df_hh.groupby('hhparcel').sum().reset_index()

print('Merge the Full Parcel File with X,Y with the parcel file from the HHs')
wrk_prcls = pd.merge(wrk_prcls, df_parcel_hh, left_on='parcelid', right_on='hhparcel', suffixes=('_x','_y'), how='left')
wrk_prcls.fillna(0,inplace=True)
wrk_prcls = wrk_prcls.drop(['hhparcel'],axis=1)
wrk_prcls.rename(columns={'emptot_p': 'employment'}, inplace=True)

print('Merge the parcels with X,Y datafame and create a column for airport buffer flag')
parcels = pd.merge(wrk_prcls, parcels_xy, on='parcelid',suffixes=('_x','_y'),how='left')
parcels.fillna(0,inplace=True)
parcels['airport_buffer'] = 0

print ('Creating a parcel layer from the x,y to spatial join with the drive times')
parcels_layer = create_point_from_table(parcels,'xcoord_p','ycoord_p',spn)

print('Flagging parcel layer with airport buffer')
buffer_cols = ['geometry']
buffer = buffer[buffer_cols]
buffer['airport'] = shed_name

airport_parcels = gp.sjoin(parcels_layer, buffer, how = "inner", op='intersects')

pop = sum(airport_parcels['population'])
jobs = sum(airport_parcels['employment'])
 
share_pop = pop /  sum(parcels['population'])
share_jobs = jobs /  sum(parcels['employment'])
