using Random
using DataFrames
using Arrow
using Tables

function nextidrange(minId, maxId, batchsize, i)
    fromId = minId + batchsize * (i-1)
    toId = min(maxId, (minId + batchsize * i)-1)
    return fromId, toId
end

minId = 1
maxId = 1000
idrange = (maxId - minId) + 1
df = DataFrame(ID=minId:maxId, B=rand(idrange), C=randstring.(fill(5,idrange)));
batchsize = 100
batches = ceil(Int32, idrange / batchsize)
partitions = Array{SubDataFrame}(undef, 0)
df1 = filter([:ID] => x -> 1 <= x <= 100, df)
df2 = filter([:ID] => x -> 101 <= x <= 200, df)
io = IOBuffer()
Arrow.write(io, Tables.partitioner([df1, df2]))
seekstart(io)
batches = Arrow.Stream(io) 
state = iterate(batches)
tt, st = state
state = iterate(batches, st)
tt, st = state
t = tt |> DataFrame
# ab = Array{DataFrame}(undef,0)
# for b in batches
#     bt = b |> DataFrame
#     push!(ab, bt)
#   for bb in b
#     bt = bb |> DataFrame
#     push!(ab, bt)
#     println("Rows = $(nrow(bt)) $(typeof(bt))")
#   end
# end
