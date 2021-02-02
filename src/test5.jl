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
numbatches = ceil(Int32, idrange / batchsize)
partitions = Array{SubDataFrame}(undef, 0)
for i = 1:numbatches 
    fromId, toId = nextidrange(minId, maxId, batchsize, i)
    push!(partitions, filter([:ID] => x -> fromId <= x <= toId, df; view = true))
end
io = IOBuffer()
Arrow.write(io, Tables.partitioner(partitions), compress=:zstd)
seekstart(io)
recordbatches = Arrow.Stream(io)
ab = Array{DataFrame}(undef,0)
for b in recordbatches 
  bt = b |> DataFrame
  println("Rows = $(nrow(bt))")
  push!(ab,bt)
end