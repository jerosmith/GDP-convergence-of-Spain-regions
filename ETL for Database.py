# IAIF-1 PROJECT
# ETL for database

# Packages
import os
import pandas as pd
import numpy as np
import datetime as dt

# Set current working directory
os.chdir('C:/_A/OneDrive/1 Work/11 Marketing/2023-01-27 Our World in Data - Data Scientist/Python OWD Job')
os.getcwd()

# Start chronometer
t0 = dt.datetime.now()

# Parameters
ruta_metadata = "Metadata/"
ruta_datos = "Datos - 3 Finales para Cargar/"
ruta_cuadratura = "Cuadratura/"
ruta_basedatos = "Base Datos/"
ruta_inter_extra_polacion = "Datos - 4 Interpolacion y Extrapolacion/"
fichero_total_fuerarango = "Total Fuera de Rango.xlsx"
fichero_total_ceutaymelilla = "Total Ceuta Melilla y Sin Informacion.xlsx"
fichero_total_interpolacion = "Total Interpolacion.xlsx"
fichero_total_extrapolacion = "Total Extrapolacion.xlsx"
fichero_modelos_extrapolacion = "Modelos Extrapolacion.xlsx"
fichero_variables = "Variables Modelo Crecimiento España.xlsx"
fichero_maestros = "Maestros.xlsx"
fichero_basedatos_sin_inter_extra_polar = "Base de Datos Sin Inter-Extrapolar.xlsx"
fichero_basedatos = "Base de Datos Modelo Crecimiento España.xlsx"
columnas_df_narrow0 = ['Comunidad Autonoma Estandarizada', 'Año', 'Valor'] # Use Python list instead of R vector
columnas_df_narrow1 = ['Año', 'Comunidad Autonoma Estandarizada', 'Variable', 'Valor'] # Use Python list instead of R vector
factor_GastoID_AAPP_2002 = 0.3954
factor_Personal_AAPP_2002 = 0.3388
variables_a_deflactar = (1013, 1020, 1077, 1078, 1079, 1080, 1081, 1082)

# IMPORT MASTER DATA TABLES AND INITIALISE NARROW GENERIC DATA FRAME

# Variables master
df_variables = pd.read_excel(ruta_metadata + fichero_variables)
df_variables['Código'] # Show column Código: values and data type.

# Autonomous communities master table
df_comunidades_autonomas = pd.read_excel(ruta_datos + fichero_maestros)
df_comunidades_autonomas['Comunidad Autonoma Estandarizada'] # Show column Comunidad Autonoma Estandarizada: values and data type.
nc = df_comunidades_autonomas.shape[1] # Number of columns

# Read variable codes from intermediate files
variables = os.listdir(path = ruta_datos) # Gets list of all file names
variables = [v[4:] for v in variables if v[:4] == 'Var_'] # Keeps only file names that start with 'Var_' and removes the 'Var_' from the names.
variables = [v[:-4] if v[-4:] == '.xls' else v for v in variables] # Replaces '.xls' with ''.
k = len(variables)

# Empty generic narrow data frame to join all variables after processing.
# Later an UNMELT operation is performed on this data frame to create the observations table.
df_narrow = pd.DataFrame(columns = ['Año', 'Comunidad Autonoma Estandarizada', 'Variable', 'Valor'])

###########################################################

# IMPORT DATA USED IN CALCULATED VARIABLES

# Expenditure on R&D as % of GDP
df_GastoIDPctPIB = pd.read_excel(ruta_datos + "Var_3.xls")
anos = df_GastoIDPctPIB.columns[1:-1]
df_GastoIDPctPIB = pd.melt(df_GastoIDPctPIB, id_vars = 'Comunidad Autónoma', value_vars = anos, var_name = "Año", value_name = "Valor")
df_GastoIDPctPIB = pd.merge(left = df_GastoIDPctPIB, right = df_comunidades_autonomas, how = 'left', left_on = "Comunidad Autónoma", right_on = 3)
df_GastoIDPctPIB = df_GastoIDPctPIB[columnas_df_narrow0]

# GDP Deflator (Base Year 2010)
df_DeflactorPIB = pd.read_excel(ruta_datos + "Var_1055.xls")
df_DeflactorPIB['Año'] = df_DeflactorPIB['Año'].astype(object) # Convert Año column to type object so that it can be used in merges

# Expenditure on R&D of IPSFL 2011 - 2016
df_IPSFL_IyD = pd.read_excel(ruta_datos + "IPSFL_IyD.xls")
df_IPSFL_IyD['Año'] = df_IPSFL_IyD['Año'].astype(object) # Convert Año column to type object so that it can be used in merges
df_IPSFL_IyD = pd.merge(left = df_IPSFL_IyD, right = df_comunidades_autonomas, how = 'left', left_on = "Comunidad Autónoma", right_on = 1006)
df_IPSFL_IyD = df_IPSFL_IyD[columnas_df_narrow0]
df_IPSFL_IyD = pd.merge(left = df_IPSFL_IyD, right = df_DeflactorPIB, how = 'left', left_on = "Año", right_on = "Año")
df_IPSFL_IyD.loc[:, 'Valor'] = df_IPSFL_IyD.loc[:, 'Valor_x'] / df_IPSFL_IyD.loc[:, 'Valor_y'] * 100
df_IPSFL_IyD = df_IPSFL_IyD[columnas_df_narrow0]

# GDP in 2010 Euros
df_105 = pd.read_excel(ruta_datos + "Var_105.xls")
anos = df_105.columns[1:-1]
df_105 = pd.melt(df_105, id_vars = 'Comunidad Autónoma', value_vars = anos, var_name = 'Año', value_name = 'Valor')
df_105 = pd.merge(left = df_105, right = df_comunidades_autonomas, how = 'left', left_on = 'Comunidad Autónoma', right_on = 105) # LEFT JOIN
df_105 = df_105[columnas_df_narrow0]
df_PIBReal2010 = pd.merge(left = df_105, right = df_DeflactorPIB, how = 'left', left_on = 'Año', right_on = 'Año')
df_PIBReal2010.loc[:, 'Valor'] = df_PIBReal2010.loc[:, 'Valor_x'] / df_PIBReal2010.loc[:, 'Valor_y'] * 100
df_PIBReal2010 = df_PIBReal2010[columnas_df_narrow0]

# Expenditure on R&D of firms
df_GastoIDEmpresas = pd.read_excel(ruta_datos + "Var_1006.xls")
df_GastoIDEmpresas['Año'] = df_GastoIDEmpresas['Año'].astype(object) # Convert Año column to type object so that it can be used in merges
df_GastoIDEmpresas = pd.merge(left = df_GastoIDEmpresas, right = df_comunidades_autonomas, how = 'left', left_on = 'Comunidad Autónoma', right_on = 1006) # LEFT JOIN
df_GastoIDEmpresas = df_GastoIDEmpresas[columnas_df_narrow0]
df_GastoIDEmpresas = pd.merge(left = df_GastoIDEmpresas, right = df_DeflactorPIB, how = 'left', left_on = 'Año', right_on = 'Año')
df_GastoIDEmpresas.loc[:, "Valor"] = df_GastoIDEmpresas.loc[:, "Valor_x"] / df_GastoIDEmpresas.loc[:, "Valor_y"] * 100
df_GastoIDEmpresas = df_GastoIDEmpresas[columnas_df_narrow0]
df_GastoIDEmpresas = pd.merge(left = df_GastoIDEmpresas, right = df_IPSFL_IyD, how = 'left', left_on = ["Año", "Comunidad Autonoma Estandarizada"], right_on = ["Año", "Comunidad Autonoma Estandarizada"])
b = np.isnan(df_GastoIDEmpresas.loc[:, "Valor_x"])
df_GastoIDEmpresas.loc[:, "Valor_x"][b] = 0
b = np.isnan(df_GastoIDEmpresas.loc[:, "Valor_y"])
df_GastoIDEmpresas.loc[:, "Valor_y"][b] = 0
df_GastoIDEmpresas.loc[:, "Valor"] = df_GastoIDEmpresas.loc[:, "Valor_x"] + df_GastoIDEmpresas.loc[:, "Valor_y"]
df_GastoIDEmpresas = df_GastoIDEmpresas[columnas_df_narrow0]

####################################################################

# PROCESS ALL FILES AND JOIN ALL INTO ONE DATA FRAME: df_narrow_todos

for i in range(k) :
    print("Processing variable " + variables[i] + ' ' + str(round((i+1)/k*100)) + ' %')
  
    # Load file of variable i into df_archivo
    cod_variable = int(variables[i])
    archivo = ruta_datos + "Var_" + variables[i] + ".xls"
    df_archivo = pd.read_excel(archivo)
    estructura = df_variables.loc[df_variables["Código"] == cod_variable, "Estructura Archivo Intermedio"].values[0]
    
    # If the file has a wide structure, convert to narrow using .melt()
    if estructura == "Wide" or estructura == "Wide_SinCA" :
        df_archivo = pd.melt(df_archivo, id_vars = "Comunidad Autónoma", var_name = "Año", value_name = "Valor")
    
    # Convert Año column of df_archivo to type object so that it can be used in merges
    df_archivo['Año'] = df_archivo['Año'].astype(object)
    
    # Standardise the autonomous communities
    # For both wide and narrow data structures
    if estructura == "Wide" or estructura == "Narrow" :
        df_archivo = pd.merge(df_archivo, df_comunidades_autonomas, how = 'left', left_on = "Comunidad Autónoma", right_on = int(variables[i])) # LEFT JOIN
        df_archivo = df_archivo[columnas_df_narrow0]

    # For structure Wide_SinCA or Narrow_SinCA
    if estructura == "Wide_SinCA" or estructura == "Narrow_SinCA" :
        df_archivo['dummykey'] = 0
        df_comunidades_autonomas['dummykey'] = 0
        df_archivo = pd.merge(df_archivo, df_comunidades_autonomas, on = 'dummykey') # CROSS JOIN
        df_archivo = df_archivo.rename(columns = {'Comunidad Autonoma Estandarizada_y':'Comunidad Autonoma Estandarizada'})
        df_archivo = df_archivo[columnas_df_narrow0]
        df_comunidades_autonomas = df_comunidades_autonomas.drop(columns = 'dummykey')

    # Special cases within the for loop
  
    # Variable 1006: Use df_GastoIDEmpresas directly
    if cod_variable == 1006 :
        df_archivo = df_GastoIDEmpresas.copy()

    # Variables to deflate: convert to 2010 euros using the GDP deflator
    if cod_variable in variables_a_deflactar :
        df_archivo = pd.merge(df_archivo, df_DeflactorPIB, how = 'left', on = "Año")
        df_archivo['Valor'] = df_archivo['Valor_x'] / df_archivo['Valor_y'] * 100
        df_archivo = df_archivo[columnas_df_narrow0]

    # Variables 1013, 1020, 1016, 1023: Correct year 2002: separate into AAPP and Univ de according to the prorate factor
    if cod_variable == 1013 or cod_variable == 1020 :
        if cod_variable == 1013 :
            df_archivo.loc[df_archivo['Año'] == 2002, "Valor"] = factor_GastoID_AAPP_2002 * df_archivo.loc[df_archivo['Año'] == 2002, "Valor"]
        else :
            df_archivo.loc[df_archivo['Año'] == 2002, "Valor"] = (1 - factor_GastoID_AAPP_2002) * df_archivo.loc[df_archivo['Año'] == 2002, "Valor"]

    if cod_variable == 1016 or cod_variable == 1023 :
        if cod_variable == 1016 :
            df_archivo.loc[df_archivo['Año'] == 2002, "Valor"] = factor_Personal_AAPP_2002 * df_archivo.loc[df_archivo['Año'] == 2002, "Valor"]
        else :
            df_archivo.loc[df_archivo['Año'] == 2002, "Valor"] = (1 - factor_Personal_AAPP_2002) * df_archivo.loc[df_archivo['Año'] == 2002, "Valor"]
    
    # Add Variable column
    nombre_variable = df_variables.loc[df_variables['Código'] == cod_variable, "Variable"].values[0]
    unidades = df_variables.loc[df_variables['Código'] == cod_variable, "Unidades"].values[0]
    df_archivo['Variable'] = nombre_variable + " (" + unidades + ")"

    # Append df_archivo to df_narrow
    df_archivo = df_archivo[columnas_df_narrow1]
    df_narrow = df_narrow.append(df_archivo)

# Special cases after for loop

# Variable 1001: Expenditure on R&D as % of GDP in 2010 euros
df_1001 = pd.merge(df_GastoIDPctPIB, df_PIBReal2010, how = 'left', on = ["Año", "Comunidad Autonoma Estandarizada"])
df_1001['Valor'] = df_1001['Valor_x'] * df_1001['Valor_y'] /100
nombre_variable = df_variables.loc[df_variables['Código'] == 1001, "Variable"].values[0]
unidades = df_variables.loc[df_variables['Código'] == 1001, "Unidades"].values[0]
df_1001['Variable'] = nombre_variable + " (" + unidades + ")"
df_1001 = df_1001[columnas_df_narrow1]
df_narrow = df_narrow.append(df_1001)

# 1007 Expenditure on R&D of firms (% of GDP)
# Variable 1007: Calculate expenditure on R&D of firms as % of GDP
df_1007 = pd.merge(df_GastoIDEmpresas, df_PIBReal2010, how = 'left', on = ["Año", "Comunidad Autonoma Estandarizada"])
df_1007['Valor'] = df_1007['Valor_x'] / df_1007['Valor_y']
nombre_variable = df_variables.loc[df_variables['Código'] == 1007, "Variable"].values[0]
unidades = df_variables.loc[df_variables['Código'] == 1007, "Unidades"].values[0]
df_1007['Variable'] = nombre_variable + " (" + unidades + ")"
df_1007 = df_1007[columnas_df_narrow1]
df_narrow = df_narrow.append(df_1007)

# 1046: Real GDP, base year 2010
df_1046 = df_PIBReal2010.copy()
nombre_variable = df_variables.loc[df_variables['Código'] == 1046, "Variable"].values[0]
unidades = df_variables.loc[df_variables['Código'] == 1046, "Unidades"].values[0]
df_1046['Variable'] = nombre_variable + " (" + unidades + ")"
df_1046 = df_1046[columnas_df_narrow1]
df_narrow = df_narrow.append(df_1046)

# Select only data from years 1998 - 2017
df_total_fuerarango = df_narrow[np.logical_not((df_narrow['Año'] >= 1998) & (df_narrow['Año'] <= 2017))]
df_total_fuerarango.to_excel(ruta_cuadratura + fichero_total_fuerarango, index=False)
df_narrow = df_narrow[(df_narrow['Año'] >= 1998) & (df_narrow['Año'] <= 2017)]

# Delete Ceuta, Melilla, Extra-region and No information
df_total_ceutaymelilla = df_narrow[df_narrow['Comunidad Autonoma Estandarizada'] == "Ceuta"]
df_total_ceutaymelilla = df_total_ceutaymelilla.append(df_narrow[df_narrow['Comunidad Autonoma Estandarizada'] == "Melilla"])
df_total_ceutaymelilla = df_total_ceutaymelilla.append(df_narrow[df_narrow['Comunidad Autonoma Estandarizada'] == "Ceuta y Melilla"])
df_total_ceutaymelilla = df_total_ceutaymelilla.append(df_narrow[df_narrow['Comunidad Autonoma Estandarizada'] == "Extra-Región"])
df_total_ceutaymelilla = df_total_ceutaymelilla.append(df_narrow[df_narrow['Comunidad Autonoma Estandarizada'] == "Sin Información"])
df_total_ceutaymelilla.to_excel(ruta_cuadratura + fichero_total_ceutaymelilla, index=False)
df_narrow = df_narrow[df_narrow['Comunidad Autonoma Estandarizada'] != "Ceuta"]
df_narrow = df_narrow[df_narrow['Comunidad Autonoma Estandarizada'] != "Melilla"]
df_narrow = df_narrow[df_narrow['Comunidad Autonoma Estandarizada'] != "Ceuta y Melilla"]
df_narrow = df_narrow[df_narrow['Comunidad Autonoma Estandarizada'] != "Extra-Región"]
df_narrow = df_narrow[df_narrow['Comunidad Autonoma Estandarizada'] != "Sin Información"]

#######################################

# CONVERT df_narrow TO WIDE (UNMELT)

# UNMELT order by Comunidad Autónoma, Year, to perform interpolation
df_observaciones = pd.pivot_table(df_narrow, values = 'Valor', index = ['Comunidad Autonoma Estandarizada', 'Año'], columns = ['Variable'])
df_observaciones = df_observaciones.reset_index(inplace=False) # Convert multi-index (non-atomic) column into columns

# Save non-interpolated data base, for comparison
df_observaciones.to_excel(ruta_inter_extra_polacion + fichero_basedatos_sin_inter_extra_polar, sheet_name = "Base Datos Sin Inter-Extrapolar", index = False)

###############

# INTERPOLATE

# Data frame to save interpolation subtotals, by comunidad autónoma, for checking purposes
df_subtotal_interpolacion = pd.DataFrame(columns = ['ComunidadAutonoma', 'Variable', 'SubtotalInterpolacion'])

nr = df_observaciones.shape[0] # N° of rows
nc = df_observaciones.shape[1] # N° of columns
i = 0
while i <= nr-1 : # Iterates over the whole data frame
    print("Performing interpolation " + str(round((i+1)/nr*100)) + "%")
    comunidad_autonoma = df_observaciones.loc[i, "Comunidad Autonoma Estandarizada"] # Saves the autonomous community in comunidad_autonoma variable
    for j in range(2, nc) : # Iterates over the variables of the autonomous community, seeking variables with intermediate NAs that hence need interpolation
        variable = df_observaciones.columns[j] # Name of column j, to save in df_subtotal_interpolacion
        subtotal_interpolacion = 0 # To save in df_subtotal_interpolacion
        # Extract specific data frame for the autonomous community and variable j
        df_ca_j = df_observaciones.loc[df_observaciones['Comunidad Autonoma Estandarizada'] == comunidad_autonoma, ['Año', variable]]
        ano_min = min(df_ca_j.loc[df_ca_j[variable].notna(), 'Año']) # Minimum year with data
        ano_max = max(df_ca_j.loc[df_ca_j[variable].notna(), 'Año']) # Maximum year with data
        imax = df_observaciones.loc[(df_observaciones['Comunidad Autonoma Estandarizada'] == comunidad_autonoma) & (df_observaciones['Año'] == ano_max), 'Año'].index[0] # Row of maximum year
        while sum(df_ca_j.loc[(df_ca_j['Año'] >= ano_min) & (df_ca_j['Año'] <= ano_max), variable].isna()) > 0 : # While variable j has intermediate NAs, perform the interpolation
        # Only if there are intermediate NAs, perform the interpolation
            i0 = min(df_observaciones[(df_observaciones['Comunidad Autonoma Estandarizada'] == comunidad_autonoma) & (df_observaciones['Año'] >= ano_min) & (df_observaciones[variable].isna())].index) # First row of the series of NAs
            i = i0
            while np.isnan(df_observaciones.iloc[i, j]) and i <= imax : # Iterate over the rows of df_observaciones up to the end of the series of NAs
                i +=1 
            i1 = i - 1 # Last row of the series of NAs
            n_NA = i1 - i0 + 1
            # Calculate the interval and increment
            valor0 = df_observaciones.iloc[i0-1, j] 
            valor1 = df_observaciones.iloc[i1+1, j]
            intervalo = valor1 - valor0
            incremento = intervalo / (n_NA+1)
            # Replace the NAs with interpolation
            for i in range(i0, i1+1) :
                df_observaciones.iloc[i, j] = valor0 + incremento*(i-i0+1)
                subtotal_interpolacion = subtotal_interpolacion + valor0 + incremento*(i-i0+1)
            # Extract specific data frame for the autonomous community and variable j
            df_ca_j = df_observaciones.loc[df_observaciones['Comunidad Autonoma Estandarizada'] == comunidad_autonoma, ['Año', variable]]
        # If the interpolation was done, save the subtotal in df_subtotal_interpolacion
        if subtotal_interpolacion != 0 :
            df_nueva_fila = pd.DataFrame({'ComunidadAutonoma' : [comunidad_autonoma], 'Variable' : [variable], 'SubtotalInterpolacion' : [subtotal_interpolacion]})
            df_subtotal_interpolacion = df_subtotal_interpolacion.append(df_nueva_fila)
    # Row of next autonomous community
    i = max(df_observaciones[df_observaciones['Comunidad Autonoma Estandarizada'] == comunidad_autonoma].index) + 1

# Aggregate df_subtotal_interpolacion into totals per variable
df_total_interpolacion = df_subtotal_interpolacion.groupby('Variable').agg({'SubtotalInterpolacion' : np.sum})
df_total_interpolacion.reset_index(inplace=True) # Get rid of the levels in the columns

# Save in Excel file
df_total_interpolacion.to_excel(ruta_cuadratura + fichero_total_interpolacion, index = False)

# SAVE FINAL DATABASE
df_observaciones.to_excel(ruta_basedatos + fichero_basedatos, index = False)

###############

# Print time taken
print("Time taken:", dt.datetime.now() - t0)


