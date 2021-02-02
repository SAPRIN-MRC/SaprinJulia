using Dates
using Arrow
using DataFrames

batches = Arrow.Stream(joinpath("D:\\Data\\SAPRIN_Data", "DIMAMO", "DayExtraction", "IndividualResidencyDays_batched.arrow"));
state = iterate(batches)
b,st = state
df = b |> DataFrame
#= state = iterate(batches, st)
b,st = state
state = iterate(batches, st)
b,st = state
state = iterate(batches, st)
b,st = state
state = iterate(batches, st)
b,st = state
state = iterate(batches, st)
b,st = state
state = iterate(batches, st)
println(state === nothing)
b,st = state
state = iterate(batches, st)
println(state === nothing)
if state !== nothing
    b,st = state
end
 =##b,state7 = iterate(batches, state6)
