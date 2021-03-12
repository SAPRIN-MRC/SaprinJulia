using Dates
using Arrow
using DataFrames
using TableBrowse

df = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data", "AHRI", "Episodes", "SurveillanceEpisodesBasic_batched.arrow")) |> DataFrame
s = filter(row -> row.Episode == 1 && row.MembershipStart == 1 && row.Born == 0 && row.Enumeration == 0 && row.InMigration == 0 && row.LocationEntry == 0 && row.ExtResStart == 0 && row.ExtResStart == 0, df)
# s = filter(row -> row.IndividualId == 151285, df)
browse(s)