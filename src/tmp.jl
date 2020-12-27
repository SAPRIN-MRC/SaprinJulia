using DataFrames
using Arrow
using Parameters
using Tables
using JSON
using Dates
using Query

#region Settings
function readsettings(f)
    return JSON.parsefile(f; dicttype=Dict, inttype=Int32, use_mmap=false)
end

s = readsettings("settings.json")
@with_kw struct Settings 
    NodeId::Int32 = s["NodeId"]
    PeriodEnd::DateTime = DateTime(s["PeriodEnd"])
    Node::String = s["Node"]
    BaseDirectory::String = s["BaseDirectory"]
    Server::String = s["Server"]
    Database::String = s["Databases"][s["Node"]]
end # struct

settings = Settings()
#endregion
# read Individuals
@time map = Arrow.Table(joinpath(settings.BaseDirectory,settings.Node,"Staging","Individuals.arrow")) |> DataFrame
map = map |> @filter(!isna(_.WomanUid)) |> DataFrame