using Dates
using Arrow
using DataFrames
using TableBrowse

df = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data", "DIMAMO", "Staging", "HHeadRelationships.arrow")) |> DataFrame
# s = filter(row -> row.InMigration == 0 && row.Enumeration == 0 && row.Born == 0 && row.LocationEntry == 0 && row.ExtResStart == 0 && row.Participation == 0 && row.MembershipStart == 0,df)
s = filter(row -> row.IndividualId == 6746, df)
browse(s)