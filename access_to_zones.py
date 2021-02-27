import inro.emme.database.emmebank as _eb
import pandas as pd
import numpy as np
import os
import h5py

#model_path = r'L:\vision2050\soundcast\dseis\integrated\final_runs\base_year\2014'
parcel_file_name = 'inputs\\scenario\\landuse\\parcels_urbansim.txt' 
model_path = r'N:\vision2050\soundcast\major_bludd - FinalRGS - Keep\2050'
output_path = r'W:\gis\projects\stefan\airport_travel_time'

am_bank = _eb.Emmebank(os.path.join(model_path, 'Banks/new_emme', '7to8', 'emmebank'))
pm_bank = _eb.Emmebank(os.path.join(model_path, 'Banks/new_emme', '17to18', 'emmebank'))

parcel_df = pd.read_csv(os.path.join(model_path, parcel_file_name), sep = ' ')
hdf_file = h5py.File(os.path.join(model_path, 'outputs\daysim\daysim_outputs.h5'))

# More than one TAZ in the list below will yield travel times to the closest one. Use just
# if you want to find accessbilility to one location (zone).
taz_list = [2074]
max_time = 60

def get_auto_information(am_bank, pm_bank):
    am_time = am_bank.matrix('svtl2t').get_numpy_data() 
    pm_time = pm_bank.matrix('svtl2t').get_numpy_data() 
    time = (am_time + pm_time) * .5

    veh_time = time[0:3700, 0:3700]
    veh_time_df = pd.DataFrame(veh_time)
    veh_time_df['from'] = veh_time_df.index
    veh_time_df = pd.melt(veh_time_df, id_vars= 'from', value_vars=list(veh_time_df.columns[0:3700]), var_name = 'to', value_name='time')
    # add 1 into zone id before join with parcel data
    veh_time_df['to'] = veh_time_df['to'] + 1 
    veh_time_df['from'] = veh_time_df['from'] + 1
    return veh_time_df

def h5_to_data_frame(h5file, integer_cols, table_name):
    table = hdf_file[table_name]
    col_dict = {}
    #cols = ['hhno', 'hhtaz']
    for col in table.iterkeys():
        #print col
        if col in integer_cols:
            my_array = np.asarray(table[col]).astype('int')
        else:
            my_array = np.asarray(table[col])
        col_dict[col] = my_array.astype(float)
    return(pd.DataFrame(col_dict))

df = get_auto_information(am_bank, pm_bank)

# multiple destinations

df = df[df['to'].isin(taz_list)]
df = df[df.time <= 60]

df = pd.DataFrame(df.groupby('from')['time'].min())
df.reset_index(inplace = True)


households = h5_to_data_frame(hdf_file, ['id'], 'Household')
persons_taz = pd.DataFrame(households.groupby('hhtaz')['hhsize'].sum())
jobs_taz = pd.DataFrame(parcel_df.groupby('TAZ_P')['EMPTOT_P'].sum())
df = df.merge(persons_taz, how = 'left', left_on = 'from', right_index = True)
df = df.merge(jobs_taz, how = 'left', left_on = 'from', right_index = True)

df.to_csv(os.path.join(output_path, 'test2.csv'))