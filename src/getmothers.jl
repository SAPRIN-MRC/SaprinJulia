using SAPRINCore
using Dates
using ODBC
using DBInterface
using DataFrames
using Arrow
using FreqTables

function loadpregnancies(db::String, node::String, basedirectory::String)
    con = ODBC.Connection(db)
    sql = """    
        SELECT
          UPPER(CONVERT(nvarchar(50),P.WomanUid)) WomanUid,
          CAST(E.EventDate AS DATE) DeliveryDate,
          CASE WHEN PO.LiveBirths >= 10 THEN 1 ELSE PO.LiveBirths END LiveBirths,
          CASE WHEN PO.StillBirths >= 10 THEN 1 ELSE PO.StillBirths END StillBirths,
          PO.TerminationTypeId
        FROM dbo.Pregnancies P
          JOIN dbo.PregnancyOutcomeEvents PO ON P.OutcomeEventUid = PO.EventUid
          JOIN dbo.Events E ON PO.EventUid = E.EventUid
    """
    pregnancies = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    @info "Read $(nrow(pregnancies)) $(node) pregnancies"
    DBInterface.close!(con)
    individualmap = Arrow.Table(joinpath(stagingpath(node), "IndividualMap.arrow")) |> DataFrame
    @info "Read $(nrow(individualmap)) $(node) individualmap entries"
    pregnancies = innerjoin(pregnancies, individualmap, on = :WomanUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    @info "Read $(nrow(pregnancies)) $(node) pregnancies after join"
    select!(pregnancies, [:IndividualId, :DeliveryDate, :LiveBirths, :StillBirths, :TerminationTypeId])
    sort!(pregnancies, [:IndividualId, :DeliveryDate])
    pregnancies = combine(groupby(pregnancies, [:IndividualId, :DeliveryDate]), :LiveBirths => maximum => :LiveBirths, :StillBirths => maximum => :StillBirths, :TerminationTypeId => maximum => :TerminationTypeId)
    Arrow.write(joinpath(basedirectory, node, "Staging", "Pregnancies.arrow"), pregnancies, compress=:zstd)
    @info "Wrote $(nrow(pregnancies)) $(node) pregnancies"
    livebirths = freqtable(pregnancies, :LiveBirths)
    @info "LiveBirths breakdown $(node)" livebirths
    return nothing
end

loadpregnancies("SaprinDb_Agincourt202012", "Agincourt", "D:\\Data\\SAPRIN_Data")
loadpregnancies("SaprinDb_AHRI202012", "AHRI", "D:\\Data\\SAPRIN_Data")
loadpregnancies("SaprinDb_DIMAMO202012", "DIMAMO", "D:\\Data\\SAPRIN_Data")