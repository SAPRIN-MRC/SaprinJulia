using Dates
using Arrow
using DataFrames
using TableBrowse

df = Arrow.Stream(joinpath("D:\\Data\\SAPRIN_Data", "DIMAMO", "DayExtraction", "DeliveryDays_batched.arrow"))
#df = Arrow.Stream(joinpath("D:\\Data\\SAPRIN_Data", "DIMAMO", "DayExtraction", "DayDatasetStep02_batched.arrow"))
#df = Arrow.Stream(joinpath("D:\\Data\\SAPRIN_Data", "Agincourt", "DayExtraction", "ResDaysNoMember_batched.arrow"))
hstate = iterate(df)
t = now()
h, hst = hstate
hd = h |> DataFrame
s = subset(hd, :MotherId => x -> x .== 1108)
browse(s)
