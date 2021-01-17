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

export readindividuals, readlocations, readresidences, readhouseholds, readhouseholdmemberships, readindividualmemberships,
       readeducationstatuses, readhouseholdsocioeconomic, readmaritalstatuses, readlabourstatuses,
       extractresidencydays, extracthhresidencydays, extractmembershipdays

#region Settings
function readsettings(f)
    return JSON.parsefile(f; dicttype=Dict, inttype=Int32, use_mmap=false)
end
@with_kw struct Settings 
    PeriodEnd::DateTime = DateTime(s["PeriodEnd"])
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
#endregion
#region startup
s = readsettings("settings.json")
settings = Settings()
createdirectories(settings.BaseDirectory, "Staging")
createdirectories(settings.BaseDirectory, "DayExtraction")
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
#endregion

include("staging.jl")
include("dayextraction.jl")

end # module
