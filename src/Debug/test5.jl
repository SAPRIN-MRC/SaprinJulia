using Dates
using Arrow
using DataFrames
using TableBrowse

df = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data", "DIMAMO", "DayExtraction", "IndividualResidencyDays1.arrow")) |> DataFrame
# s = filter(row -> row.IndividualId == 151285, df)
browse(df)