using DataFrames
using Arrow
using Dates
using Statistics
using ShiftedArrays
using Missings
using CategoricalArrays
using TableOperations
using Tables

function individualbatch(basedirectory, node, batchsize)
    individualmap = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualMap.arrow")) |> DataFrame
    minId = minimum(individualmap[!,:IndividualId])
    maxId = maximum(individualmap[!,:IndividualId])
    idrange = (maxId - minId) + 1
    batches = ceil(Int32, idrange / batchsize)
    @info "Node $(node) Batch size $(batchsize) Minimum id $(minId), maximum Id $(maxId), idrange $(idrange), batches $(batches)"
    return minId, maxId, batches
end
function nextidrange(minId, maxId, batchsize, i)
    fromId = minId + batchsize * (i-1)
    toId = min(maxId, (minId + batchsize * i)-1)
    return fromId, toId
end
function batchonindividualid(basedirectory, node, file, batchsize)
    minId, maxId, numbatches = individualbatch(basedirectory, node, batchsize)
    df = Arrow.Table(joinpath(basedirectory,node,"DayExtraction","$(file).arrow"))
    println("Read memberships arrow table", now())
    partitions = Array{TableOperations.Filter}(undef, 0)
    for i = 1:numbatches 
        fromId, toId = nextidrange(minId, maxId, batchsize, i)
        println("Batch $(i) from $(fromId) to $(toId)")
        push!(partitions, TableOperations.filter(x -> fromId <= x.IndividualId <= toId, df))
    end
    println("Starting to write")
    open(joinpath(basedirectory,node,"DayExtraction","$(file)_batched.arrow"),"a"; lock = true) do io
        Arrow.write(io, Tables.partitioner(partitions), compress=:zstd)
    end
end

@info "Started execution $(now())"
t = now()
batchonindividualid("D:\\Data\\SAPRIN_Data","Agincourt", "IndividualResidencyDays", 20000)
@info "Finished Agincourt $(now())"
batchonindividualid("D:\\Data\\SAPRIN_Data","DIMAMO", "IndividualResidencyDays", 20000)
@info "Finished DIMAMO $(now())"
batchonindividualid("D:\\Data\\SAPRIN_Data","AHRI", "IndividualResidencyDays", 20000)
d = now()-t
@info "Stopped execution $(now()) duration $(round(d, Dates.Second))"
