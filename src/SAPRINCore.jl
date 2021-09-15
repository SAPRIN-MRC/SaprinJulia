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
using StataCall

export BatchSize, individualbatch, nextidrange, addsheet!, writeXLSX, arrowtocsv, stagingpath, dayextractionpath, episodepath, settings, age, arrowtostata,
       readindividuals, readlocations, readresidences, readhouseholds, readhouseholdmemberships, readindividualmemberships, readpregnancies,
       readeducationstatuses, readhouseholdsocioeconomic, readmaritalstatuses, readlabourstatuses,
       extractresidencydays, extracthhresidencydays, extractmembershipdays, combinebatches, deliverydays,
       preferredhousehold, setresidencyflags, addindividualattributes, mothercoresident, fathercoresident,
       basicepisodes, basicepisodeQA, yrage_episodes, yrage_episodeQA, yragedel_episodes, yragedel_episodeQA
       
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
        mkpath(joinpath(basedirectory, "AHRI"))
    end
    if !isdir(joinpath(basedirectory, "AHRI", directory))
        mkpath(joinpath(basedirectory, "AHRI", directory))
    end
    if !isdir(joinpath(basedirectory, "DIMAMO"))
        mkpath(joinpath(basedirectory, "DIMAMO"))
    end
    if !isdir(joinpath(basedirectory, "DIMAMO", directory))
        mkpath(joinpath(basedirectory, "DIMAMO", directory))
    end
    if !isdir(joinpath(basedirectory, "Agincourt"))
        mkpath(joinpath(basedirectory, "Agincourt"))
    end
    if !isdir(joinpath(basedirectory, "Agincourt", directory))
        mkpath(joinpath(basedirectory, "Agincourt", directory))
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
s = readsettings(joinpath(pwd(),"src","settings.json"))
settings = Settings()
createdirectories(settings.BaseDirectory, "Staging")
createdirectories(settings.BaseDirectory, "DayExtraction")
createdirectories(settings.BaseDirectory, joinpath("Episodes","QC"))
#endregion
#region Utility functions
function age(dob::Date, date::Date)
    return Dates.year(date) - Dates.year(dob) - ((Dates.Month(date) < Dates.Month(dob)) || (Dates.Month(date) == Dates.Month(dob) && Dates.Day(date) < Dates.Day(dob)))
end
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
function individualbatch(node::String, batchsize::Int64 = BatchSize)
    individualmap = Arrow.Table(joinpath(stagingpath(node),"IndividualMap.arrow")) |> DataFrame
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
function readpartitionfile(file::String; lock = true)
    open(file, "r"; lock = lock) do io
        return Arrow.Table(io)
    end
end
"Concatenate record batches"
function combinebatches(path::String, file::String, batches)
    files = Array{String,1}()
    for i = 1:batches
        push!(files,joinpath(path, "$(file)$(i).arrow"))
    end
    Arrow.write(joinpath(path, "$(file)_batched.arrow"), Tables.partitioner(x->readpartitionfile(x),files), compress=:zstd)
    #delete chunks
    for i = 1:batches
        rm(joinpath(path, "$(file)$(i).arrow"))
    end
    return nothing
end #combinebatches
"Add a sheet to an exisiting Excel spreadsheet and transfer the contents of df NAmedArray to the sheet"
function addsheet!(path::String, df::NamedArray, sheetname::String)
    XLSX.openxlsx(path, mode ="rw") do xf
        sheet = XLSX.addsheet!(xf, sheetname)
        data = collect([NamedArrays.names(df)[1], df.array])
        cnames = [String(dimnames(df)[1]), "n"]
        XLSX.writetable!(sheet, data, cnames)
    end
    return nothing
end
"Add a sheet to an existing Excel spreadsheet and transfer the contents of df DataFrame to the sheet"
function addsheet!(path::String, df::AbstractDataFrame, sheetname::String)
    if nrow(df) == 0
        return nothing
    end
    transform!(df, names(df, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    transform!(df, names(df, Int16) .=> ByRow(Int), renamecols=false) #needed by XLSX
    transform!(df, names(df, Int32) .=> ByRow(Int), renamecols=false) #needed by XLSX
    XLSX.openxlsx(path, mode ="rw") do xf
        sheet = XLSX.addsheet!(xf, sheetname)
        data = collect(eachcol(df))
        cnames = DataFrames.names(df)
        XLSX.writetable!(sheet, data, cnames)
    end
    return nothing
end
"Write a dataframe to an Excel spreadsheet, overwrite file if it exists"
function writeXLSX(path::String, df::AbstractDataFrame, sheetname::String)
    transform!(df, names(df, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    transform!(df, names(df, Int16) .=> ByRow(Int), renamecols=false) #needed by XLSX
    transform!(df, names(df, Int32) .=> ByRow(Int), renamecols=false) #needed by XLSX
    XLSX.writetable(path, collect(eachcol(df)), names(df), overwrite = true, sheetname = sheetname)
end
"Write dataset as CSV"
function arrowtocsv(node::String, subdir::String, dataset::String)
    Arrow.Table(joinpath(settings.BaseDirectory, node, subdir, "$(dataset).arrow")) |> CSV.write(joinpath(settings.BaseDirectory, node, subdir, "$(dataset).csv"))
    return nothing
end
"Convert episode file in Arrow format to Stata"
function arrowtostata(node, inputfile, outputfile)
    df = readpartitionfile(joinpath(episodepath(node), inputfile * ".arrow"), lock = false) |> DataFrame
    statafile = joinpath(episodepath(node), outputfile * ".dta")
    cmds = ["compress", "saveold \"$(statafile)\", replace"]
    stataCall(cmds, df, false, false, true)
    return nothing
end
#endregion

include("staging.jl")
include("dayextraction.jl")
include("episodecreation.jl")

end # module
