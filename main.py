import pandas as pd
import os

coverage_universe_list = ["Diamondback"]
directory = r'C:\Users\brand\OneDrive - Novilabs\Novi Intelligence\all_upstream_data\data\us-onshore'

# Initialize the basin list
basin_list = (
    'Barnett', 'Delaware', 'DJ', 'Eagle Ford', 'Fayetteville',
    'Haynesville', 'Marcellus', 'Midland', 'Other', 'Powder River',
    'San Juan', 'SCOOP STACK', 'Uinta', 'Utica', 'Williston'
)

ticker_dict = {"Diamondback": "FANG"}

def load_in_all_data(basin_list, base_path):
    all_data = {}
    for basin in basin_list:
        if basin not in ["Midland", "Delaware"]:
            file_path = os.path.join(base_path, basin, "All subbasins", "Bulk", "Database", "WellDetails.tsv")
            prod_path = os.path.join(base_path, basin, "All subbasins", "Bulk", "Database", "ForecastWellMonths.tsv")
        else:
            file_path = os.path.join(base_path, "Permian", basin, "Bulk", "Database", "WellDetails.tsv")
            prod_path = os.path.join(base_path, "Permian", basin, "Bulk", "Database", "ForecastWellMonths.tsv")

        well_details_df = pd.read_csv(file_path, sep='\t')
        prod_df = pd.read_csv(prod_path, sep='\t')
        
        all_data[basin] = {
            "well_details_df": well_details_df,
            "prod_df": prod_df
        }
    return all_data

def pull_operated_prod(operator, all_data):
    basin_dfs = []  # Define basin_dfs inside the function
    operator_results = {}  # Initialize operator_results

    for basin, data in all_data.items():
        well_details_df = data["well_details_df"]
        prod_df = data["prod_df"]

        # Filter well details to rows where CurrentOperator equals operator
        operator_wells = well_details_df[well_details_df["CurrentOperator"] == operator]

        # Skip if operator not found in the basin
        if operator_wells.empty:
            continue

        # Get the unique API10 values for these wells
        operator_api10_list = operator_wells["API10"].unique().tolist()

        # Filter production DataFrame to those API10s
        filtered_prod_df = prod_df[prod_df["API10"].isin(operator_api10_list)].copy()

        # Append this filtered DataFrame to our list
        basin_dfs.append(filtered_prod_df)

    # Concatenate data from all basins for the operator
    if basin_dfs:
        final_df = pd.concat(basin_dfs, ignore_index=True)
    else:
        final_df = pd.DataFrame()

    # Reorder columns to match your desired output, if they exist
    desired_columns = [
        "API10", "Date", "MonthsOnProduction", "IsForecasted", "Basin", "Subbasin", 
        "OilPerDay", "OilPerMonth", "CumulativeOil", 
        "GasPerDay", "GasPerMonth", "CumulativeGas",
        "WaterPerDay", "WaterPerMonth", "CumulativeWater",
        "CreatedAt", "ModifiedAt"
    ]
    final_df = final_df[[col for col in desired_columns if col in final_df.columns]]

    operator_results[operator] = final_df

    # Melt per-day streams into long format
    melted = operator_results[operator].melt(
        id_vars=["API10", "Basin", "Subbasin", "Date"],
        value_vars=["OilPerDay", "GasPerDay", "WaterPerDay"],
        var_name="Stream",
        value_name="Value"
    )

    # Group and sum by Basin, Subbasin, Stream, Date
    grouped = (
        melted
        .groupby(["Basin", "Subbasin", "Stream", "Date"], as_index=False)
        .Value.sum()
    )

    # Pivot so each Date becomes its own column
    pivoted = grouped.pivot(
        index=["Basin", "Subbasin", "Stream"],
        columns="Date",
        values="Value"
    ).reset_index()

    return pivoted

# Use 'directory' as the base path
all_data = load_in_all_data(basin_list, directory)

for operator in coverage_universe_list:
    ticker = ticker_dict[str(operator)]

    # Collect data from each basin for this operator
    pivoted_data_df = pull_operated_prod(operator, all_data)

    # Define the export path with proper string formatting
    export_base_path = fr'C:\Users\brand\OneDrive - Novilabs\Novi Intelligence\Economic Models\{ticker}.xlsx'

    # Write the DataFrame to the Operated_PDP sheet
    with pd.ExcelWriter(export_base_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        pivoted_data_df.to_excel(writer, sheet_name="Operated_PDP", index=False)
