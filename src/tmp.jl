using DataFrames
using Arrow
using Dates
using Statistics
using ShiftedArrays
using Missings
using XLSX
using CategoricalArrays


function householdsocioeconomic(db::String, node::String, basedirectory::String)
    con = ODBC.Connection(db)
    sql = """SELECT
        UPPER(CONVERT(varchar(50),HouseholdObservationUid)) HouseholdObservationUid,
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
    println("Read $(nrow(s)) $(node) HSE observations from database")
    householdmap = Arrow.Table(joinpath(basedirectory,node,"Staging","HouseholdMap.arrow")) |> DataFrame
    si = innerjoin(s, householdmap, on=:HouseholdUid => :HouseholdUid, makeunique=true, matchmissing=:equal)
    println("Read $(nrow(si)) $(node) HSE observations after household map")
    householdassets = Arrow.Table(joinpath(basedirectory,node,"Staging","AssetStatus.arrow")) |> DataFrame
    s = leftjoin(si,householdassets,on = :HouseholdObservationUid => :HouseholdObservationUid, makeunique=true, matchmissing=:equal)
    select!(s,Not([:HouseholdObservationUid,:HouseholdUid]))
    a = freqtable(s,:WaterSource)
    println("Watersource breakdown for $(node)")
    show(a)
    println()
    recode!(s[!,:WaterSource], missing =>0, 1 =>4, 2 => 3, 3 => 2, 4 => 1, 5 => 1, 6 => 1, 7 => 1, 8 => 1, 9 => 0, 10 => 2, 11 => 1)
    a = freqtable(s,:WaterSource)
    println()
    println("Watersource breakdown for $(node) after recode")
    show(a)
    println()
    a = freqtable(s,:Toilet)
    println("Toilet breakdown for $(node)")
    show(a)
    println()
    recode!(s[!,:Toilet], missing => 0, 0 => 0, 1 => 3, [2,5] => 2, [3, 4] => 1)
    a = freqtable(s,:Toilet)
    println()
    println("Toilet breakdown for $(node) after recode")
    show(a)
    println()
    a = freqtable(s,:CookingFuel)
    println("CookingFuel breakdown for $(node)")
    show(a)
    println()
    recode!(s[!,:CookingFuel], missing => 0, 0 => 0, 1 => 1, 2 => 4, 3 => 2, 4 => 5, [5,6] => 3)
    a = freqtable(s,:CookingFuel)
    println()
    println("CookingFuel breakdown for $(node) after recode")
    show(a)
    println()
    a = freqtable(s,:WallMaterial)
    println("WallMaterial breakdown for $(node)")
    show(a)
    println()
    recode!(s[!,:WallMaterial], missing => 0, 0 => 0, [1, 2] => 4, 3 => 3, 4 => 2, [5, 6, 7] => 1)
    a = freqtable(s,:WallMaterial)
    println()
    println("WallMaterial breakdown for $(node) after recode")
    show(a)
    println()
    a = freqtable(s,:FloorMaterial)
    println("FloorMaterial breakdown for $(node)")
    show(a)
    println()
    recode!(s[!,:FloorMaterial], missing => 0, 0 => 0, [1, 2, 8, 9] => 3, [3, 6, 10] => 2, [11, 12, 13] => 1)
    a = freqtable(s,:FloorMaterial)
    println()
    println("FloorMaterial breakdown for $(node) after recode")
    show(a)
    println()
    a = freqtable(s,:Bedrooms)
    println("Bedrooms breakdown for $(node)")
    show(a)
    println()
    recode!(s[!,:Bedrooms], missing => 0, 0 => 0, [1, 2] => 1, [3, 4] => 2, [5, 6] => 3, 7:90 => 4, 91:99 => 0, 100:9999 => 4)
    a = freqtable(s,:Bedrooms)
    println()
    println("Bedrooms breakdown for $(node) after recode")
    show(a)
    println()
    a = freqtable(s,:ConnectedToGrid)
    println("ConnectedToGrid breakdown for $(node)")
    show(a)
    println()
    replace!(s.ConnectedToGrid, missing => 0, true => 1, false => 0)
    a = freqtable(s,:ConnectedToGrid)
    println()
    println("ConnectedToGrid for $(node) after recode")
    show(a)
    println()
    transform!(s,[:Bedrooms,:WallMaterial,:FloorMaterial] => ByRow((b,w,f) -> (b/4 + w/4 + f/3)/3) => :DwellingIdx, 
                 [:WaterSource,:Toilet] => ByRow((w,t) -> (w/4 + t/3)/2) => :WaterSanitationIdx, 
                 [:ConnectedToGrid, :CookingFuel] => ByRow((x,y) -> ((x + y/5)/2)) => :PowerSupplyIdx,
                 [:Livestock] => ByRow(x -> x/2) => :LivestockIdx,
                 [:Modern] => ByRow(x -> x/9) => :ModernAssetIdx)
    transform!(s,[:DwellingIdx,:WaterSanitationIdx,:PowerSupplyIdx,:LivestockIdx, :ModernAssetIdx] => ByRow((a,b,c,d,e) -> (a + b + c + d + e)) => :SEIdx)
    select!(s, [:HouseholdId,:ObservationDate,:SEIdx,:DwellingIdx,:WaterSanitationIdx,:PowerSupplyIdx,:LivestockIdx, :ModernAssetIdx, 
                :Crime, :FinancialStatus, :CutMeals, :CutMealsFrequency, :NotEat, :NotEatFrequency, :ChildMealSkipCut, :ChildMealSkipCutFrequency, :ConsentToCall])
    sort!(s, [:HouseholdId,:ObservationDate])
    Arrow.write(joinpath(basedirectory, node, "Staging", "SocioEconomic.arrow"), s, compress=:zstd)
    return nothing
end
node = "DIMAMO"
df = householdsocioeconomic(settings.Databases[node], node, settings.BaseDirectory)
