module SAPRINCore

using Parameters
using Dates
using ODBC
using DBInterface
using DataFrames
using JSON
using FreqTables
using Arrow
using DataValues
using ShiftedArrays
using XLSX
using Statistics
using CategoricalArrays
using CSV
using NamedArrays

export BatchSize, individualbatch, nextidrange, addsheet!, arrowtocsv, stagingpath, dayextractionpath, episodepath,
       readindividuals, readlocations, readresidences, readhouseholds, readhouseholdmemberships, readindividualmemberships,
       readeducationstatuses, readhouseholdsocioeconomic, readmaritalstatuses, readlabourstatuses,
       extractresidencydays, extracthhresidencydays, extractmembershipdays, combinebatches, 
       preferredhousehold, setresidencyflags, addindividualattributes,
       basicepisodes, basicepisodeQA
#region Constants
const BatchSize = 20000
#endregion
#region Settings
function readsettings(f)
    return JSON.parsefile(f; dicttype=Dict, inttype=Int32, use_mmap=false)
end
@with_kw struct Settings 
    PeriodEnd::DateTime = DateTime(s["PeriodEnd"])
    LTFCutOff::DateTime = DateTime(s["LTFCutOff"])
    BaseDirectory::String = s["BaseDirectory"]
    Server::String = s["Server"]
    Databases = s["Databases"]
    LeftCensorDates = s["LeftCensorDates"]
    Nodes = s["Nodes"]
end # struct
"Create directories to output staging data"
function createdirectories(basedirectory,directory)
    if !isdir(joinpath(basedirectory, "AHRI"))
        mkdir(joinpath(basedirectory, "AHRI"))
    end
    if !isdir(joinpath(basedirectory, "AHRI", directory))
        mkdir(joinpath(basedirectory, "AHRI", directory))
    end
    if !isdir(joinpath(basedirectory, "DIMAMO"))
        mkdir(joinpath(basedirectory, "DIMAMO"))
    end
    if !isdir(joinpath(basedirectory, "DIMAMO", directory))
        mkdir(joinpath(basedirectory, "DIMAMO", directory))
    end
    if !isdir(joinpath(basedirectory, "Agincourt"))
        mkdir(joinpath(basedirectory, "Agincourt"))
    end
    if !isdir(joinpath(basedirectory, "Agincourt", directory))
        mkdir(joinpath(basedirectory, "Agincourt", directory))
    end
    return nothing
end
function stagingpath(node::String)
    return joinpath(settings.BaseDirectory,node,"Staging")
end
function dayextractionpath(node::String)
    return joinpath(settings.BaseDirectory,node,"DayExtraction")
end
function episodepath(node::String)
    return joinpath(settings.BaseDirectory,node,"Episodes")
end
#endregion
#region startup
s = readsettings("settings.json")
settings = Settings()
createdirectories(settings.BaseDirectory, "Staging")
createdirectories(settings.BaseDirectory, "DayExtraction")
createdirectories(settings.BaseDirectory, "Episodes")
#endregion
#region Utility functions
"Constrain date a to be no larger than b"
function rightcensor(a::DataValue{Date}, b::Date)::Date
    return rightcensor(get(a, b), b) # get returns underlying Date, with default b if a is missing (isna)
end
function rightcensor(a::Date, b::Date)
    return a <= b ? a : b
end
"Returns a random date relative to base, constraint by start and end date"
function newrandomdate(base::Date, startdate::Date, enddate::Date)
    return base + Dates.Day(trunc(Int64, Dates.value(enddate - startdate) * rand(Float64)))
end
function convertanytoint(a)
    return convert(Int64, a)
end
function convertanytostr(a)
    return string(a)
end
function individualbatch(basedirectory, node, batchsize::Int64 = BatchSize)
    individualmap = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualMap.arrow")) |> DataFrame
    minId = minimum(individualmap[!,:IndividualId])
    maxId = maximum(individualmap[!,:IndividualId])
    idrange = (maxId - minId) + 1
    batches = ceil(Int32, idrange / batchsize)
    @info "Node $(node) Batch size $(batchsize) Minimum id $(minId), maximum Id $(maxId), idrange $(idrange), batches $(batches)"
    return minId, maxId, batches
end
function nextidrange(minId, maxId, i, batchsize::Int64 = BatchSize)
    fromId = minId + batchsize * (i-1)
    toId = min(maxId, (minId + batchsize * i)-1)
    return fromId, toId
end
function readpartitionfile(file)
    open(file,"r"; lock = true) do io
        return Arrow.Table(io)
    end
end
"Concatenate record batches"
function combinebatches(basedirectory::String, node::String, subdir::String, file::String, batches)
    files = Array{String,1}()
    for i = 1:batches
        push!(files,joinpath(basedirectory, node, subdir, "$(file)$(i).arrow"))
    end
    Arrow.write(joinpath(basedirectory, node, subdir, "$(file)_batched.arrow"), Tables.partitioner(x->readpartitionfile(x),files), compress=:zstd)
    #delete chunks
    for i = 1:batches
        rm(joinpath(basedirectory, node, subdir, "$(file)$(i).arrow"))
    end
    return nothing
end #combinebatches
"Add a sheet to an exisiting Excel spreadsheet and transfer the contents of df NAmedArray to the sheet"
function addsheet!(path, df::NamedArray, sheetname)
    XLSX.openxlsx(path, mode ="rw") do xf
        sheet = XLSX.addsheet!(xf, sheetname)
        data = collect([NamedArrays.names(df)[1], df.array])
        cnames = [String(dimnames(df)[1]), "n"]
        XLSX.writetable!(sheet, data, cnames)
    end
end
"Add a sheet to an exisiting Excel spreadsheet and transfer the contents of df DataFrame to the sheet"
function addsheet!(path, df::AbstractDataFrame, sheetname)
    XLSX.openxlsx(path, mode ="rw") do xf
        sheet = XLSX.addsheet!(xf, sheetname)
        data = collect(eachcol(df))
        cnames = DataFrames.names(df)
        XLSX.writetable!(sheet, data, cnames)
    end
end
"Write dataset as CSV"
function arrowtocsv(node::String, subdir::String, dataset::String)
    Arrow.Table(joinpath(settings.BaseDirectory, node, subdir, "$(dataset).arrow")) |> CSV.write(joinpath(settings.BaseDirectory, node, subdir, "$(dataset).csv"))
    return nothing
end

#endregion

include("staging.jl")
include("dayextraction.jl")
include("episodecreation.jl")

end # module
