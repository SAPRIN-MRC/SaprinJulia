using ODBC
using DataFrames
using XLSX
using DBInterface

function convertanytoint(a)
    return convert(Int64, a)
end
function convertanytostr(a)
    return string(a)
end

function householdassets(db::String, node::String)
    con = ODBC.Connection(db)
    sql = """SELECT
        UPPER(CONVERT(varchar(50),HA.HouseholdObservationUid)) HouseholdObservationUid,
        HA.AssetId,
        HA.AssetStatusId
    FROM dbo.HouseholdAssets HA
    WHERE HA.AssetStatusId>0;
    """
    s =  DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(s)) $(node) asset statuses from database"
    assetmap = DataFrame(XLSX.readtable(joinpath(pwd(),"src","Assets.xlsx"),"Consolidated")...)
    assetmap[!,:AssetId] = map(convertanytoint,assetmap[!,:AssetId])
    assetmap[!,:Id] = map(convertanytoint,assetmap[!,:Id])
    assetmap[!,:AssetName] = map(convertanytostr,assetmap[!,:AssetName])
    assetmap[!,:Name] = map(convertanytostr,assetmap[!,:Name])
    assetmap[!,:AssetIdx] = map(convertanytostr,assetmap[!,:AssetIdx])    
    si = innerjoin(s, assetmap, on = :AssetId => :AssetId,  makeunique=true, matchmissing=:equal)
     g = combine(groupby(si,[:HouseholdObservationUid,:Id]), :AssetStatusId => minimum => :AssetStatus, :AssetIdx => first => :AssetGroup)
    @info "$(nrow(g)) $(node) grouped asset statuses"
    filter!([:AssetStatus] => x -> x == 1, g)
    @info "$(nrow(g)) $(node) present asset statuses"
    gg = combine(groupby(g,[:HouseholdObservationUid,:AssetGroup]), :AssetStatus => sum => :Idx)
    @info "$(nrow(gg)) $(node) asset groups"
     filter!([:AssetGroup] => x -> x != "0", gg)
    w = unstack(gg, :HouseholdObservationUid, :AssetGroup, :Idx)
    replace!(w.Modern, missing => 0)
    replace!(w.Livestock, missing => 0)
    disallowmissing!(w,[:HouseholdObservationUid,:Modern,:Livestock])
    #Arrow.write(joinpath(stagingpath(node), "AssetStatus.arrow"), w, compress=:zstd)
    return w
end #householdassets

df = householdassets("SaprinDb_AHRI202206", "AHRI")