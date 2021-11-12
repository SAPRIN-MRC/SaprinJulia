using SAPRINCore
using Dates
using Arrow
using DataFrames
using ShiftedArrays
using XLSX
using FreqTables
using ODBC

function convertanytoint(a)
    return convert(Int64, a)
end
function convertanytostr(a)
    return string(a)
end

"Retrieve asset items"
function retrieve_asset_items(db::String, node::String)
    con = ODBC.Connection(db)
    sql = """
    SELECT
        UPPER(CONVERT(varchar(50),HO.HouseholdUid)) HouseholdUid,
        CONVERT(date,E.EventDate) ObservationDate,
        HA.AssetId,
        HA.AssetStatusId
    FROM dbo.HouseholdAssets HA
    JOIN dbo.HouseholdObservations HO ON HA.HouseholdObservationUid = HO.HouseholdObservationUid
    JOIN dbo.Events E ON HO.ObservationUid=E.EventUid
    WHERE HA.AssetStatusId>0;
    """
    s =  DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(s)) $(node) asset statuses from database"
    householdmap = Arrow.Table(joinpath(stagingpath(node), "HouseholdMap.arrow")) |> DataFrame
    si = innerjoin(s, householdmap, on=:HouseholdUid => :HouseholdUid, makeunique=true, matchmissing=:equal)
    disallowmissing!(si, :ObservationDate)
    select!(si, :HouseholdId, :ObservationDate, :AssetId, :AssetStatusId)
    Arrow.write(joinpath(stagingpath(node), "AssetStatusRaw.arrow"), si, compress=:zstd)
    return nothing
end
"Retrieve household socioeconmic variables"
function retrieve_hse(db::String, node::String)
    con = ODBC.Connection(db)
    sql = """SELECT
        UPPER(CONVERT(varchar(50),HouseholdUid)) HouseholdUid,
        CONVERT(date,E.EventDate) ObservationDate,
        WaterSource,
        Toilet,
        ConnectedToGrid,
        CookingFuel,
        WallMaterial,
        FloorMaterial,
        Bedrooms,
        Crime,
        FinancialStatus,
        CutMeals,
        CutMealsFrequency,
        NotEat,
        NotEatFrequency,
        ChildMealSkipCut,
        ChildMealSkipCutFrequency,
        ConsentToCall
    FROM dbo.HouseholdObservations HO
        JOIN dbo.Events E ON HO.ObservationUid = E.EventUid;
    """
    s =  DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(s)) $(node) HSE observations from database"
    householdmap = Arrow.Table(joinpath(stagingpath(node), "HouseholdMap.arrow")) |> DataFrame
    si = innerjoin(s, householdmap, on=:HouseholdUid => :HouseholdUid, makeunique=true, matchmissing=:equal)
    @info "Read $(nrow(si)) $(node) HSE observations after household map"
    select!(si, Not([:HouseholdUid]))
    sort!(si, [:HouseholdId,:ObservationDate])
    Arrow.write(joinpath(stagingpath(node), "SocioEconomicRaw.arrow"), si, compress=:zstd)
    return nothing
end

@info "Started execution $(now())"
t = now()
node = "Agincourt"
retrieve_asset_items(settings.Databases[node], node)
arrowtostatar(node, stagingpath(node), "AssetStatusRaw", "AssetStatusRaw")
runstata("assets.do", settings.Version, node, joinpath(stagingpath(node), "AssetStatusRaw.dta"))
retrieve_hse(settings.Databases[node], node)
arrowtostatar(node, stagingpath(node), "SocioEconomicRaw", "SocioEconomicRaw")
runstata("socioeconomic.do", settings.Version, node, joinpath(stagingpath(node), "SocioEconomicRaw.dta"))
@info "Finished $(node) $(now())"
d = now()-t
@info "Stopped $(node) execution $(now()) duration $(round(d, Dates.Second))"
