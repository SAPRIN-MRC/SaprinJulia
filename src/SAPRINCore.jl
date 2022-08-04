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
# using StataCall - causing need for old version of DataFrames
using RCall

export BatchSize, individualbatch, nextidrange, addsheet!, writeXLSX, arrowtocsv, stagingpath, dayextractionpath, episodepath, settings, age, 
       arrowtostatar, runstata,
       readindividuals, readlocations, readresidences, readhouseholds, readhouseholdmemberships, readindividualmemberships, readpregnancies,
       readeducationstatuses, readhouseholdsocioeconomic, readmaritalstatuses, readlabourstatuses,
       extractresidencydays, extracthhresidencydays, extractmembershipdays, combinebatches, deliverydays,
       preferredhousehold, setresidencyflags, addindividualattributes, mothercoresident, fathercoresident,
       basicepisodes, basicepisodeQA, yrage_episodes, yrage_episodeQA, yragedel_episodes, yragedel_episodeQA,
       yragedelparentalstatus_episodes
#region Constants
const BatchSize = 25000
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
    Version::VersionNumber = VersionNumber(s["Version"])
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
"Concatenate record batches in separate Arrow files to a single partitioned Arrow file"
function combinebatches(path::String, file::String, batches)
    files = Array{String,1}()
    for i = 1:batches
        push!(files,joinpath(path, "$(file)$(i).arrow"))
    end
    arrow_parts = Tables.partitioner(Arrow.Table, files)
    open(joinpath(path, "$(file)_batched.arrow"), "w") do io
        Arrow.write(io, arrow_parts, ntasks = 1, compress=:zstd)
    end
    arrow_parts = nothing
    GC.gc()
    #delete chunks
    for i = 1:batches
        rm(joinpath(path, "$(file)$(i).arrow"))
    end
    return nothing
end #combinebatches
"Combines a set of serialized DataFrames into a single DataFrame and save it in Arrow format"
function combineserializedbatches(path::String, file::String, batches)
    i = 1
    df = open(f -> Serialization.deserialize(f), joinpath(path, "$(file)$(i).jls"), "r")
    while i < batches
        i = i + 1
        s = open(f -> Serialization.deserialize(f), joinpath(path, "$(file)$(i).jls"), "r")
        df = vcat(df, s)
    end
    open(joinpath(path, "$(file).arrow"), "w") do io
        Arrow.write(io, df, compress=:zstd)
    end
    df = nothing
    GC.gc()
    #delete chunks
    for i = 1:batches
        rm(joinpath(path, "$(file)$(i).jls"))
    end
    return nothing
end #combineserializedbatches
"Add a sheet to an existing Excel spreadsheet and transfer the contents of df NamedArray to the sheet"
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
    transform!(df, names(df, Union{Int16, Missing}) .=> ByRow(passmissing(Int64)), renamecols=false) #needed by XLSX
    transform!(df, names(df, Int32) .=> ByRow(Int), renamecols=false) #needed by XLSX
    transform!(df, names(df, Union{Int32, Missing}) .=> ByRow(passmissing(Int64)), renamecols=false) #needed by XLSX
    XLSX.openxlsx(path, mode ="rw") do xf
        sheet = XLSX.addsheet!(xf, sheetname)
        data = collect(eachcol(df))
        cnames = DataFrames.names(df)
        XLSX.writetable!(sheet, data, cnames)
    end
    return nothing
end
function writeXLSX(path::String, df::NamedArray, sheetname::String)
    XLSX.writetable(path, collect([NamedArrays.names(df)[1], df.array]), [String(dimnames(df)[1]), "n"], overwrite = true, sheetname = sheetname)
end
"Write a dataframe to an Excel spreadsheet, overwrite file if it exists"
function writeXLSX(path::String, df::AbstractDataFrame, sheetname::String)
    if !isdir(dirname(path))
        mkpath(dirname(path))
    end
    transform!(df, names(df, Int8) .=> ByRow(Int64), renamecols=false) #needed by XLSX
    transform!(df, names(df, Int16) .=> ByRow(Int64), renamecols=false) #needed by XLSX
    transform!(df, names(df, Union{Int16, Missing}) .=> ByRow(passmissing(Int64)), renamecols=false) #needed by XLSX
    transform!(df, names(df, Int32) .=> ByRow(Int64), renamecols=false) #needed by XLSX
    transform!(df, names(df, Union{Int32, Missing}) .=> ByRow(passmissing(Int64)), renamecols=false) #needed by XLSX
    XLSX.writetable(path, collect(eachcol(df)), names(df), overwrite = true, sheetname = sheetname)
    return nothing
end
"Write Arrow file as CSV"
function arrowtocsv(node::String, subdir::String, dataset::String)
    Arrow.Table(joinpath(settings.BaseDirectory, node, subdir, "$(dataset).arrow")) |> CSV.write(joinpath(settings.BaseDirectory, node, subdir, "$(dataset).gzip"), compress = true)
    return nothing
end
#"Convert episode file in Arrow format to Stata - deprecated using StatCall"
# function arrowtostata(node, inputfile, outputfile)
#     df = readpartitionfile(joinpath(episodepath(node), inputfile * ".arrow"), lock = false) |> DataFrame
#     statafile = joinpath(episodepath(node), outputfile * ".dta")
#     cmds = ["compress", "la da \"SAPRIN Episodes v4\"", "saveold \"$(statafile)\", replace"]
#     stataCall(cmds, df, false, false, true)
#     return nothing
# end
"Convert file in Arrow format to Stata using R"
function arrowtostatar(path, inputfile, outputfile)
    df = Arrow.Table(joinpath(path, inputfile * ".arrow")) |> DataFrame
    arrowfile = joinpath(path, outputfile * "_tmp.arrow")
    Arrow.write(arrowfile, df, compress = :zstd)
    statafile = joinpath(path, outputfile * ".dta")
    R"""
    library(arrow)
    library(rio)
    x <- read_feather($arrowfile)
    export(x, $statafile)
    """
    df = nothing
    GC.gc()
    rm(arrowfile)
    return nothing
end
"""
Execute a STATA do-file, STATA_BIN environment variable must be set
Parameters:
  dofile: STATA dofile to be executed, can contain the fullpath to the file, if not will try to find the file in ./src/dofiles
  version: The dataset version number
  node: The SAPRIN node associated with the datafile
  datafile: the path to the datafile including the .dta file extension,  will be substituted in the dofile, usually in the "use" statement
Substitutions:
  #datafile# with datafile
  #node# with node
  #version# with version
"""
function runstata(dofile::String, version::VersionNumber, node::String, datafile::String)
    stata_executable = ""
    if haskey(ENV, "STATA_BIN")
        stata_executable = ENV["STATA_BIN"]
    else
        error("Could not find the Stata executable. Please set the \"STATA_BIN\" environment variable.")
    end
    if !isfile(dofile)
        #try looking for file in src\dofiles
        dofile = joinpath(pwd(), "src", "dofiles", dofile)
        if !isfile(dofile)
            error("Can't find do file '$dofile'")
        end
    end
    #Replace version number and node in do_file
    dolines = readlines(dofile)
    for i in 1:length(dolines)
        dolines[i] = replace(dolines[i], "#datafile#" => datafile)
        dolines[i] = replace(dolines[i], "#node#" => node)
        dolines[i] = replace(dolines[i], "#version#" => "$(version)")
    end
    fname = tempname() * ".do"
    open(fname,"w") do f
        for i in 1:length(dolines)
            println(f, dolines[i])
        end
    end
    run(`"$stata_executable" /e do $fname`)
    #println(fname)
    rm(fname)
end
#endregion

include("staging.jl")
include("dayextraction.jl")
include("episodecreation.jl")

end # module
