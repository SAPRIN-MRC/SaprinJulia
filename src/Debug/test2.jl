using Dates
using Arrow
using DataFrames
using TableBrowse

df = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data", "DIMAMO", "Staging", "IndividualResidencies.arrow")) |> DataFrame
# s = filter(row -> row.InMigration == 0 && row.Enumeration == 0 && row.Born == 0 && row.LocationEntry == 0 && row.ExtResStart == 0 && row.Participation == 0 && row.MembershipStart == 0,df)
s = filter(row -> row.IndividualId == 184, df)
#s = filter(row -> row.StartDate > row.EndDate, df)
#s = subset(df,[:StartDate, :EndDate] => (x,y) -> x>y, skipmissing=true)
browse(s)